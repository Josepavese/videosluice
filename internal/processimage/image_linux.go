//go:build linux

package processimage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

type imageID struct {
	major, minor uint64
	inode        uint64
}
type region struct {
	id                 imageID
	start, end, offset uint64
	perms, path        string
}

// Owner holds separate read-only aliases. Closing these mappings never unlocks
// a caller's existing VMAs or another Owner, even when they share file pages.
type Owner struct {
	mu    sync.Mutex
	pages [][]byte
	bytes uint64
}

func (o *Owner) Bytes() uint64 {
	if o == nil {
		return 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.bytes
}
func (o *Owner) Close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	var result error
	var retained [][]byte
	var retainedBytes uint64
	for i := len(o.pages) - 1; i >= 0; i-- {
		if err := unix.Munmap(o.pages[i]); err != nil {
			result = errors.Join(result, err)
			retained = append(retained, o.pages[i])
			retainedBytes += uint64(len(o.pages[i]))
		}
	}
	o.pages, o.bytes = retained, retainedBytes
	return result
}

func parseRegion(line string) (region, error) {
	var r region
	fields := make([]string, 0, 5)
	for range 5 {
		line = strings.TrimLeft(line, " \t")
		i := strings.IndexAny(line, " \t")
		if i < 0 {
			fields = append(fields, line)
			line = ""
		} else {
			fields = append(fields, line[:i])
			line = line[i:]
		}
	}
	r.path = strings.TrimLeft(line, " \t")
	bounds := strings.Split(fields[0], "-")
	device := strings.Split(fields[3], ":")
	if len(bounds) != 2 || len(device) != 2 || len(fields[1]) != 4 {
		return r, errors.New("invalid process image map")
	}
	values := []*uint64{&r.start, &r.end, &r.offset, &r.id.major, &r.id.minor, &r.id.inode}
	for i, text := range []string{bounds[0], bounds[1], fields[2], device[0], device[1], fields[4]} {
		base := 16
		if i == 5 {
			base = 10
		}
		n, err := strconv.ParseUint(text, base, 64)
		if err != nil {
			return r, fmt.Errorf("invalid process image map: %w", err)
		}
		*values[i] = n
	}
	r.perms = fields[1]
	if r.end <= r.start {
		return r, errors.New("invalid process image range")
	}
	return r, nil
}

func acquire(ctx context.Context, budget uint64) (*Owner, error) {
	if ctx == nil {
		return nil, errors.New("nil process image context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if budget == 0 {
		return &Owner{}, nil
	}
	f, err := os.Open("/proc/self/maps")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var maps []region
	images := make(map[imageID]bool)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		r, err := parseRegion(scanner.Text())
		if err != nil {
			return nil, err
		}
		if r.id.inode == 0 || !strings.HasPrefix(r.path, "/") {
			continue
		}
		maps = append(maps, r)
		if r.perms[2] == 'x' {
			images[r.id] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lockRegions(ctx, selectImages(maps, images), budget)
}

func selectImages(maps []region, images map[imageID]bool) []region {
	var selected []region
	for _, r := range maps {
		if images[r.id] && r.perms[0] == 'r' && r.perms[1] != 'w' {
			selected = append(selected, r)
		}
	}
	sort.Slice(selected, func(i, j int) bool {
		a, b := selected[i], selected[j]
		if a.id.major != b.id.major {
			return a.id.major < b.id.major
		}
		if a.id.minor != b.id.minor {
			return a.id.minor < b.id.minor
		}
		if a.id.inode != b.id.inode {
			return a.id.inode < b.id.inode
		}
		return a.offset < b.offset
	})
	// Different VMAs (including another Owner's aliases) may cover the same
	// file pages. Reserve their union once; never recursively reserve aliases.
	union := selected[:0]
	for _, r := range selected {
		if len(union) > 0 {
			previous := &union[len(union)-1]
			previousEnd := previous.offset + previous.end - previous.start
			nextEnd := r.offset + r.end - r.start
			if previous.id == r.id && r.offset <= previousEnd {
				if nextEnd > previousEnd {
					previous.end += nextEnd - previousEnd
				}
				continue
			}
		}
		union = append(union, r)
	}
	return union
}

func lockRegions(ctx context.Context, regions []region, budget uint64) (owner *Owner, result error) {
	o := &Owner{}
	defer func() {
		if result != nil {
			cleanupErr := o.Close()
			result = errors.Join(result, cleanupErr)
			if cleanupErr != nil {
				owner = o
			}
		}
	}()
	var total uint64
	for _, r := range regions {
		size := r.end - r.start
		if r.end <= r.start || size > budget || total > budget-size || size > uint64(^uint(0)>>1) || r.offset > uint64(^uint64(0)>>1) {
			return nil, errors.New("process image exceeds residency budget")
		}
		total += size
	}
	if total == 0 {
		return nil, errors.New("no immutable process images found")
	}
	for _, r := range regions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := os.Open(r.path)
		if err != nil {
			return nil, fmt.Errorf("open process image: %w", err)
		}
		var stat unix.Stat_t
		err = unix.Fstat(int(f.Fd()), &stat)
		if err == nil && (stat.Ino != r.id.inode || uint64(unix.Major(uint64(stat.Dev))) != r.id.major || uint64(unix.Minor(uint64(stat.Dev))) != r.id.minor || stat.Mode&unix.S_IFMT != unix.S_IFREG) {
			err = errors.New("process image identity changed")
		}
		var pages []byte
		if err == nil {
			pages, err = unix.Mmap(int(f.Fd()), int64(r.offset), int(r.end-r.start), unix.PROT_READ, unix.MAP_SHARED)
		}
		closeErr := f.Close()
		if pages != nil {
			o.pages = append(o.pages, pages)
		}
		if err != nil || closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		if err = unix.Mlock(pages); err != nil {
			return nil, fmt.Errorf("lock process image: %w", err)
		}
		o.bytes += uint64(len(pages))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return o, nil
}
