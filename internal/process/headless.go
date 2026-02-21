package process

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"

	"claude-hub/internal/config"
	"claude-hub/pkg/claude"
)

// HeadlessProcess represents a headless Claude process
type HeadlessProcess struct {
	SessionID       string
	ClaudeSessionID string // The actual Claude session UUID
	CWD             string
	Cmd             *exec.Cmd
	Stdin           io.WriteCloser
	Stdout          io.ReadCloser
	Stderr          io.ReadCloser
	StartedAt       time.Time
	IsGenerating    bool

	// Channels for output
	OutputChan chan *claude.StreamMessage
	ErrorChan  chan error

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	mu sync.RWMutex
}

// NewHeadlessProcess creates a new headless Claude process
func NewHeadlessProcess(sessionID, cwd, claudeSessionID string) (*HeadlessProcess, error) {
	ctx, cancel := context.WithCancel(context.Background())

	cmd := []string{
		"claude",
		"--print",
		"--verbose",
		"--dangerously-skip-permissions",
		"--output-format", "stream-json",
		"--input-format", "stream-json",
	}

	if claudeSessionID != "" {
		cmd = append(cmd, "--resume", claudeSessionID)
	}

	if cwd == "" {
		cwd = config.Get().WorkDir
	}

	execCmd := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
	execCmd.Dir = cwd

	stdin, err := execCmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	stdout, err := execCmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := execCmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := execCmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to start Claude process: %w", err)
	}

	hp := &HeadlessProcess{
		SessionID:       sessionID,
		ClaudeSessionID: claudeSessionID,
		CWD:             cwd,
		Cmd:             execCmd,
		Stdin:           stdin,
		Stdout:          stdout,
		Stderr:          stderr,
		StartedAt:       time.Now(),
		OutputChan:      make(chan *claude.StreamMessage, 256),
		ErrorChan:       make(chan error, 10),
		ctx:             ctx,
		cancel:          cancel,
	}

	// Start reading stdout and stderr
	go hp.readStdout()
	go hp.readStderr()

	log.Printf("[%s] Started headless Claude process (PID: %d)", sessionID, execCmd.Process.Pid)

	return hp, nil
}

// SendMessage sends a message to Claude
func (hp *HeadlessProcess) SendMessage(content interface{}) error {
	hp.mu.Lock()
	defer hp.mu.Unlock()

	msg := map[string]interface{}{
		"type": "user",
		"message": map[string]interface{}{
			"role":    "user",
			"content": content,
		},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	hp.IsGenerating = true

	if _, err := hp.Stdin.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to stdin: %w", err)
	}

	log.Printf("[%s] Sent message to Claude", hp.SessionID)
	return nil
}

// Kill terminates the Claude process
func (hp *HeadlessProcess) Kill() error {
	hp.mu.Lock()
	defer hp.mu.Unlock()

	log.Printf("[%s] Killing headless Claude process", hp.SessionID)
	hp.cancel()

	if hp.Cmd.Process != nil {
		return hp.Cmd.Process.Kill()
	}

	return nil
}

// Wait waits for the process to exit
func (hp *HeadlessProcess) Wait() error {
	return hp.Cmd.Wait()
}

// readStdout reads and parses stdout from Claude
func (hp *HeadlessProcess) readStdout() {
	defer close(hp.OutputChan)

	scanner := bufio.NewScanner(hp.Stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var msg claude.StreamMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			log.Printf("[%s] Error parsing stdout JSON: %v (line: %s)", hp.SessionID, err, line)
			continue
		}

		// Capture Claude session ID from init message
		if msg.Type == "system" && msg.Subtype == "init" && msg.SessionID != "" {
			hp.mu.Lock()
			hp.ClaudeSessionID = msg.SessionID
			hp.mu.Unlock()
			log.Printf("[%s] Captured Claude session ID: %s", hp.SessionID, msg.SessionID)
		}

		// Mark generation as complete on result
		if msg.Type == "result" {
			hp.mu.Lock()
			hp.IsGenerating = false
			hp.mu.Unlock()
		}

		// Send to output channel
		select {
		case hp.OutputChan <- &msg:
		case <-hp.ctx.Done():
			return
		}
	}

	if err := scanner.Err(); err != nil {
		select {
		case hp.ErrorChan <- fmt.Errorf("stdout scanner error: %w", err):
		case <-hp.ctx.Done():
		}
	}
}

// readStderr reads stderr from Claude
func (hp *HeadlessProcess) readStderr() {
	scanner := bufio.NewScanner(hp.Stderr)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			// Log stderr but don't fail on it
			log.Printf("[%s] Claude stderr: %s", hp.SessionID, line)
		}
	}
}

// GetClaudeSessionID returns the Claude session ID
func (hp *HeadlessProcess) GetClaudeSessionID() string {
	hp.mu.RLock()
	defer hp.mu.RUnlock()
	return hp.ClaudeSessionID
}

// Done returns the context's done channel
func (hp *HeadlessProcess) Done() <-chan struct{} {
	return hp.ctx.Done()
}
