package process

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"claude-hub/internal/config"
)

// ClaudeProcess represents a detected Claude process
type ClaudeProcess struct {
	PID       int
	SessionID string // Extracted from open file descriptors
}

// TerminalDetector scans for Claude processes running in terminals
type TerminalDetector struct {
	projectDir    string
	ownPIDs       map[int]bool // PIDs of our own headless processes
}

// NewTerminalDetector creates a new terminal detector
func NewTerminalDetector() *TerminalDetector {
	return &TerminalDetector{
		projectDir: config.Get().ClaudeProjectsDir,
		ownPIDs:    make(map[int]bool),
	}
}

// RegisterOwnPID marks a PID as belonging to our headless processes
func (d *TerminalDetector) RegisterOwnPID(pid int) {
	d.ownPIDs[pid] = true
}

// UnregisterOwnPID removes a PID from our tracking
func (d *TerminalDetector) UnregisterOwnPID(pid int) {
	delete(d.ownPIDs, pid)
}

// ScanForTerminalSessions scans /proc for Claude processes
func (d *TerminalDetector) ScanForTerminalSessions() ([]ClaudeProcess, error) {
	var processes []ClaudeProcess
	var claudeProcessCount int

	// Read /proc directory
	procDir, err := os.Open("/proc")
	if err != nil {
		return nil, fmt.Errorf("failed to open /proc: %w", err)
	}
	defer procDir.Close()

	entries, err := procDir.Readdirnames(-1)
	if err != nil {
		return nil, fmt.Errorf("failed to read /proc: %w", err)
	}

	for _, entry := range entries {
		// Check if entry is a number (PID)
		pid, err := strconv.Atoi(entry)
		if err != nil {
			continue
		}

		// Skip our own processes
		if d.ownPIDs[pid] {
			continue
		}

		// Check if this is a Claude process
		if !d.isClaudeProcess(pid) {
			continue
		}

		claudeProcessCount++

		// Try to extract session ID from open files
		sessionID := d.extractSessionID(pid)
		if sessionID != "" {
			processes = append(processes, ClaudeProcess{
				PID:       pid,
				SessionID: sessionID,
			})
		} else {
			// Debug: log Claude processes without session ID
			log.Printf("Found Claude process PID %d but couldn't extract session ID", pid)
		}
	}

	if claudeProcessCount > 0 {
		log.Printf("Found %d Claude processes total, %d with session IDs", claudeProcessCount, len(processes))
	}

	return processes, nil
}

// isClaudeProcess checks if a PID is running the claude command
func (d *TerminalDetector) isClaudeProcess(pid int) bool {
	cmdlinePath := filepath.Join("/proc", strconv.Itoa(pid), "cmdline")

	data, err := os.ReadFile(cmdlinePath)
	if err != nil {
		return false
	}

	// cmdline is null-separated
	parts := strings.Split(string(data), "\x00")
	if len(parts) == 0 {
		return false
	}

	// Check if the command is "claude"
	cmd := filepath.Base(parts[0])
	return cmd == "claude"
}

// extractSessionID tries to find which .jsonl file this process has open
func (d *TerminalDetector) extractSessionID(pid int) string {
	fdDir := filepath.Join("/proc", strconv.Itoa(pid), "fd")

	// Read all file descriptors
	fds, err := os.ReadDir(fdDir)
	if err != nil {
		log.Printf("Cannot read fd dir for PID %d: %v", pid, err)
		return ""
	}

	var foundFiles []string
	for _, fd := range fds {
		// Read the symlink to see what file it points to
		linkPath := filepath.Join(fdDir, fd.Name())
		target, err := os.Readlink(linkPath)
		if err != nil {
			continue
		}

		foundFiles = append(foundFiles, target)

		// Check if it's a .jsonl file in our project directory
		if strings.HasPrefix(target, d.projectDir) && strings.HasSuffix(target, ".jsonl") {
			// Extract the UUID from the filename
			base := filepath.Base(target)
			sessionID := strings.TrimSuffix(base, ".jsonl")
			log.Printf("PID %d has session file: %s -> UUID: %s", pid, target, sessionID)
			return sessionID
		}
	}

	if len(foundFiles) > 0 {
		log.Printf("PID %d open files (none matched): %v", pid, foundFiles[:min(5, len(foundFiles))])
	}

	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// FindSessionProcess finds if there's a terminal process for a specific session
func (d *TerminalDetector) FindSessionProcess(sessionID string) (*ClaudeProcess, error) {
	processes, err := d.ScanForTerminalSessions()
	if err != nil {
		return nil, err
	}

	for _, proc := range processes {
		if proc.SessionID == sessionID {
			return &proc, nil
		}
	}

	return nil, nil
}

// IsProcessRunning checks if a PID is still running
func IsProcessRunning(pid int) bool {
	processPath := filepath.Join("/proc", strconv.Itoa(pid))
	_, err := os.Stat(processPath)
	return err == nil
}
