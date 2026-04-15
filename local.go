package sessioncast

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// ExecLocal runs a command locally via tmux without a relay server.
//
//	result, err := sessioncast.ExecLocal(ctx, &ExecLocalSpec{
//	    Command:       "ls -la",
//	    Cwd:           "/home/user",
//	    IdleThreshold: 3 * time.Second,
//	    MaxWait:       30 * time.Second,
//	    ExitPattern:   `\$\s*$`,
//	})
func ExecLocal(ctx context.Context, spec *ExecLocalSpec) (*ExecResult, error) {
	if spec.Command == "" {
		return nil, fmt.Errorf("command is required")
	}

	sessionName := fmt.Sprintf("sessioncast-local-%d", time.Now().UnixMilli())

	// Defaults
	idleThreshold := spec.IdleThreshold
	if idleThreshold == 0 {
		idleThreshold = 3 * time.Second
	}
	maxWait := spec.MaxWait
	if maxWait == 0 {
		maxWait = 30 * time.Second
	}

	// Create tmux session
	createArgs := []string{"new-session", "-d", "-s", sessionName, "-x", "120", "-y", "40"}
	if spec.Cwd != "" {
		createArgs = append(createArgs, "-c", spec.Cwd)
	}
	if err := runTmux(createArgs...); err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	defer runTmux("kill-session", "-t", sessionName)

	// Send command
	if err := runTmux("send-keys", "-t", sessionName, spec.Command, "Enter"); err != nil {
		return nil, fmt.Errorf("send keys: %w", err)
	}

	// Wait for completion
	var exitPattern *regexp.Regexp
	if spec.ExitPattern != "" {
		exitPattern = regexp.MustCompile(spec.ExitPattern)
	}

	start := time.Now()
	lastContent := ""
	lastChangeTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if time.Since(start) > maxWait {
			content := capturePane(sessionName)
			return &ExecResult{Stdout: content, ExitCode: -1}, nil
		}

		content := capturePane(sessionName)
		if content == "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		changed := content != lastContent
		if changed {
			lastContent = content
			lastChangeTime = time.Now()
		}

		// Exit pattern match
		if exitPattern != nil && exitPattern.MatchString(content) {
			return &ExecResult{Stdout: content, ExitCode: 0, Duration: time.Since(start).Milliseconds()}, nil
		}

		// Idle detection
		if !changed && time.Since(lastChangeTime) > idleThreshold {
			return &ExecResult{Stdout: content, ExitCode: 0, Duration: time.Since(start).Milliseconds()}, nil
		}

		if changed {
			time.Sleep(50 * time.Millisecond)
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// CapturePane captures the current terminal content of a tmux session.
func CapturePane(sessionName string) string {
	return capturePane(sessionName)
}

// CapturePaneAnsi captures with ANSI escape sequences.
func CapturePaneAnsi(sessionName string) string {
	out, err := exec.Command("tmux", "capture-pane", "-pt", sessionName, "-e", "-N").Output()
	if err != nil {
		return ""
	}
	return string(out)
}

// ExecLocalSpec configures a local execution.
type ExecLocalSpec struct {
	Command       string
	Cwd           string
	IdleThreshold time.Duration // Default: 3s
	MaxWait       time.Duration // Default: 30s
	ExitPattern   string        // Regex to detect command completion
}

func capturePane(sessionName string) string {
	out, err := exec.Command("tmux", "capture-pane", "-pt", sessionName).Output()
	if err != nil {
		return ""
	}
	return strings.TrimRight(string(out), "\n ")
}

func runTmux(args ...string) error {
	return exec.Command("tmux", args...).Run()
}
