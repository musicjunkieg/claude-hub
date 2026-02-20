package watcher

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"claude-hub/internal/config"

	"github.com/fsnotify/fsnotify"
)

// SessionWatcher watches a Claude session file for changes
type SessionWatcher struct {
	SessionID   string
	FilePath    string
	Offset      int64

	watcher     *fsnotify.Watcher
	events      chan *ParsedMessage
	stop        chan struct{}
	mu          sync.RWMutex
}

// NewSessionWatcher creates a new session watcher
func NewSessionWatcher(sessionID, claudeUUID string) (*SessionWatcher, error) {
	// Construct path to .jsonl file
	projectDir := config.Get().ClaudeProjectsDir
	filePath := filepath.Join(projectDir, claudeUUID+".jsonl")

	// Check if file exists
	info, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("session file not found: %w", err)
	}

	// Create fsnotify watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	sw := &SessionWatcher{
		SessionID: sessionID,
		FilePath:  filePath,
		Offset:    info.Size(), // Start from end of file
		watcher:   watcher,
		events:    make(chan *ParsedMessage, 256),
		stop:      make(chan struct{}),
	}

	// Watch the file
	if err := watcher.Add(filePath); err != nil {
		watcher.Close()
		return nil, fmt.Errorf("failed to watch file: %w", err)
	}

	log.Printf("[%s] Started watching: %s (offset: %d)", sessionID, filePath, sw.Offset)

	return sw, nil
}

// Start begins watching for file changes
func (sw *SessionWatcher) Start() {
	go sw.watchLoop()
}

// Stop stops the watcher
func (sw *SessionWatcher) Stop() {
	close(sw.stop)
	sw.watcher.Close()
	close(sw.events)
}

// Events returns the channel of parsed events
func (sw *SessionWatcher) Events() <-chan *ParsedMessage {
	return sw.events
}

// watchLoop is the main watch loop
func (sw *SessionWatcher) watchLoop() {
	for {
		select {
		case event, ok := <-sw.watcher.Events:
			if !ok {
				return
			}

			if event.Op&fsnotify.Write == fsnotify.Write {
				sw.handleFileChange()
			}

		case err, ok := <-sw.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("[%s] Watcher error: %v", sw.SessionID, err)

		case <-sw.stop:
			return
		}
	}
}

// handleFileChange reads new content from the file
func (sw *SessionWatcher) handleFileChange() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	file, err := os.Open(sw.FilePath)
	if err != nil {
		log.Printf("[%s] Error opening file: %v", sw.SessionID, err)
		return
	}
	defer file.Close()

	// Seek to last known offset
	if _, err := file.Seek(sw.Offset, 0); err != nil {
		log.Printf("[%s] Error seeking file: %v", sw.SessionID, err)
		return
	}

	// Read new lines
	scanner := bufio.NewScanner(file)
	linesRead := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		// Parse JSONL line
		msg, err := ParseJSONLLine(line)
		if err != nil {
			log.Printf("[%s] Error parsing line: %v", sw.SessionID, err)
			sw.Offset += int64(len(line)) + 1
			continue
		}

		// Extract displayable content
		parsed, err := ExtractContent(msg)
		if err != nil {
			log.Printf("[%s] Error extracting content: %v", sw.SessionID, err)
			sw.Offset += int64(len(line)) + 1
			continue
		}

		// Send to events channel if we have content
		if parsed != nil {
			select {
			case sw.events <- parsed:
				linesRead++
			case <-sw.stop:
				return
			}
		}

		// Update offset
		sw.Offset += int64(len(line)) + 1
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[%s] Scanner error: %v", sw.SessionID, err)
	}

	if linesRead > 0 {
		log.Printf("[%s] Read %d new messages from file", sw.SessionID, linesRead)
	}
}

// GetOffset returns the current file offset
func (sw *SessionWatcher) GetOffset() int64 {
	sw.mu.RLock()
	defer sw.mu.RUnlock()
	return sw.Offset
}
