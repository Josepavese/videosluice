//go:build linux

package broker

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Observe the owned, not-yet-Waited child BEFORE cancellation destroys the
// startup evidence. This is diagnostic only, never an SDK release receipt.
// No argv, environment, frame content or arbitrary kernel text enters the log.
func brokerBootstrapObservation(cmd *exec.Cmd) string {
	if cmd == nil || cmd.Process == nil || cmd.Process.Pid <= 0 || cmd.ProcessState != nil {
		return "startup_observation=unavailable"
	}
	root := "/proc/" + strconv.Itoa(cmd.Process.Pid)
	status, err := readBootstrapProc(root+"/status", 16384)
	if err != nil {
		return "startup_observation=unavailable"
	}
	wchan, _ := readBootstrapProc(root+"/wchan", 128)
	return formatBootstrapObservation(status, wchan)
}

func readBootstrapProc(path string, limit int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit {
		return "", fmt.Errorf("startup proc observation unavailable")
	}
	return string(data), nil
}

func formatBootstrapObservation(status, wchan string) string {
	values := make(map[string]string, 4)
	for _, line := range strings.Split(status, "\n") {
		name, value, ok := strings.Cut(line, ":")
		if ok && (name == "State" || name == "Threads" || name == "SigCgt" || name == "SigBlk") {
			values[name] = strings.TrimSpace(value)
		}
	}
	state := "unknown"
	if s := values["State"]; len(s) > 0 && strings.ContainsRune("RSDTtXZPI", rune(s[0])) && (len(s) == 1 || s[1] == ' ' || s[1] == '\t') {
		state = s[:1]
	}
	threads := "unknown"
	if n, err := strconv.ParseUint(values["Threads"], 10, 32); err == nil {
		threads = strconv.FormatUint(n, 10)
	}
	signalBit := func(name string) string {
		n, err := strconv.ParseUint(values[name], 16, 64)
		if err != nil {
			return "unknown"
		}
		return strconv.FormatBool(n&(1<<14) != 0) // Linux SIGTERM == 15.
	}
	wchan = strings.TrimSpace(wchan)
	if len(wchan) == 0 || len(wchan) > 96 || strings.IndexFunc(wchan, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_')
	}) >= 0 {
		wchan = "unavailable"
	}
	return fmt.Sprintf("startup_observation=proc state=%s threads=%s term_caught=%s term_blocked=%s wait_channel=%s",
		state, threads, signalBit("SigCgt"), signalBit("SigBlk"), wchan)
}
