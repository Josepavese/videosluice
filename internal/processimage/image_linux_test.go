//go:build linux

package processimage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func fixtureRegion(t *testing.T) region {
	t.Helper()
	p := filepath.Join(t.TempDir(), "program with  two spaces")
	if err := os.WriteFile(p, make([]byte, os.Getpagesize()), 0600); err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Stat(p, &stat); err != nil {
		t.Fatal(err)
	}
	return region{id: imageID{uint64(unix.Major(uint64(stat.Dev))), uint64(unix.Minor(uint64(stat.Dev))), stat.Ino}, start: 0x10000, end: 0x10000 + uint64(os.Getpagesize()), perms: "r-xp", path: p}
}
func lockedBytes(t *testing.T) uint64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmLck:") {
			n, err := strconv.ParseUint(strings.Fields(line)[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n * 1024
		}
	}
	t.Fatal("missing VmLck")
	return 0
}
func TestParseRegionPreservesIdentityAndPath(t *testing.T) {
	r, err := parseRegion("00400000-00401000 r-xp 00002000 b3:02 123  /some path/with  two spaces")
	if err != nil || r.offset != 0x2000 || r.id != (imageID{0xb3, 2, 123}) || r.path != "/some path/with  two spaces" {
		t.Fatalf("%+v %v", r, err)
	}
	for _, line := range []string{"", "00400000-00400000 r-xp 0 b3:02 123 /p", "00400000-00401000 r-xp bad! b3:02 123 /p"} {
		if _, err := parseRegion(line); err == nil {
			t.Fatal("accepted malformed map", line)
		}
	}
}
func TestResidencyKernelOwnershipAndAdmission(t *testing.T) {
	// A separate process makes VmLck, RLIMIT and the initial resident set private.
	if os.Getenv("VIDEOSLUICE_RESIDENCY_TEST_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestResidencyKernelOwnershipAndAdmission$")
		cmd.Env = append(os.Environ(), "VIDEOSLUICE_RESIDENCY_TEST_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v", out, err)
		}
		return
	}
	ctx := context.Background()
	r := fixtureRegion(t)
	page := uint64(os.Getpagesize())
	before := lockedBytes(t)
	// Existing operator locks must survive both another owner's Close and a
	// partial admission failure. Each alias charges its own VMA to this process.
	first, err := lockRegions(ctx, []region{r}, page)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := lockRegions(ctx, []region{r}, page)
	if err != nil {
		t.Fatal(err)
	}
	if first.Bytes() != page || second.Bytes() != page || lockedBytes(t) != before+2*page {
		t.Fatal("missing kernel memlock accounting")
	}
	// Discard an independent original VMA's PTE and request eviction of this
	// private, fsynced fixture's clean cache. The locked file-page aliases must
	// keep that original image resident without touching its lock ownership.
	file, err := os.OpenFile(r.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Sync(); err != nil {
		t.Fatal(err)
	}
	original, err := unix.Mmap(int(file.Fd()), 0, int(page), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Madvise(original, unix.MADV_DONTNEED); err != nil {
		t.Fatal(err)
	}
	if err = unix.Fadvise(int(file.Fd()), 0, int64(page), unix.FADV_DONTNEED); err != nil {
		t.Fatal(err)
	}
	var residency byte
	if _, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&original[0])), uintptr(page), uintptr(unsafe.Pointer(&residency))); errno != 0 || residency&1 == 0 {
		t.Fatalf("locked alias failed to retain image cache: resident=%d errno=%v", residency, errno)
	}
	if err = unix.Munmap(original); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
	if second.Bytes() != 0 || lockedBytes(t) != before+page {
		t.Fatal("Close changed another owner's lock")
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
	bad := r
	bad.id.inode++
	if o, err := lockRegions(ctx, []region{r, bad}, 2*page); err == nil || o != nil {
		t.Fatal("changed identity admitted")
	}
	if lockedBytes(t) != before+page {
		t.Fatal("partial admission leaked memlock")
	}
	if o, err := lockRegions(ctx, []region{r}, page-1); err == nil || o != nil {
		t.Fatal("budget overrun admitted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := lockRegions(cancelled, []region{r}, page); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	if lockedBytes(t) != before {
		t.Fatal("owner leaked locks")
	}
	var limit unix.Rlimit
	if err = unix.Getrlimit(unix.RLIMIT_MEMLOCK, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = 0
	if err = unix.Setrlimit(unix.RLIMIT_MEMLOCK, &limit); err != nil {
		t.Fatal(err)
	}
	// Root may have CAP_IPC_LOCK, so run the quota refusal in the same unprivileged
	// CI/user context as the daemon. Do not silently skip a missing refusal.
	if os.Geteuid() != 0 {
		if o, err := lockRegions(ctx, []region{r}, page); err == nil || o != nil {
			t.Fatal("insufficient memlock admitted")
		}
	}
	if lockedBytes(t) != before {
		t.Fatal("failed admission leaked locks")
	}
	fmt.Println("kernel ownership, partial cleanup, budget and cancellation verified")
}
func TestAcquireStartupImagesBounded(t *testing.T) {
	// Verify actual selection without requiring CI's small memlock limit to fit
	// the entire Go test executable. Admission must reject before mapping pages.
	before := lockedBytes(t)
	if owner, err := Acquire(context.Background(), 1); err == nil || owner != nil {
		t.Fatal("startup image exceeded budget")
	}
	if lockedBytes(t) != before {
		t.Fatal("budget check allocated locks")
	}
	disabled, err := Acquire(context.Background(), 0)
	if err != nil || disabled.Bytes() != 0 {
		t.Fatal(err)
	}
	if err = disabled.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(nil, 0); err == nil {
		t.Fatal("nil context admitted")
	}
}

func TestSelectionExcludesDataAndMergesImageAliases(t *testing.T) {
	image := region{id: imageID{1, 2, 3}, start: 0x1000, end: 0x3000, offset: 0, perms: "r-xp", path: "/program"}
	alias := image
	alias.start = 0x8000
	alias.end = 0xa000
	alias.perms = "r--s"
	constants := image
	constants.start = 0x3000
	constants.end = 0x4000
	constants.offset = 0x2000
	constants.perms = "r--p"
	writable := image
	writable.start = 0x4000
	writable.end = 0x5000
	writable.offset = 0x3000
	writable.perms = "rw-p"
	model := image
	model.id.inode++
	model.path = "/model"
	model.perms = "r--p"
	selected := selectImages([]region{model, alias, writable, constants, image}, map[imageID]bool{image.id: true})
	if len(selected) != 1 || selected[0].offset != 0 || selected[0].end-selected[0].start != 0x3000 {
		t.Fatalf("wrong image union: %+v", selected)
	}
}
