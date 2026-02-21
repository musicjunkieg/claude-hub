package session

import (
	"encoding/json"
	"sync"
	"time"

	"claude-hub/internal/config"
)

// SessionState represents the state of a session
type SessionState int

const (
	StateIdle SessionState = iota
	StateWebOnly
	StateTerminalOnly
	StateTransitioning
)

// StreamingState tracks the current streaming state for reconnecting clients
type StreamingState struct {
	ActiveContentBlocks []json.RawMessage // Active content blocks being streamed
	IsGenerating        bool              // Whether Claude is currently generating
}

// Session represents a Claude Code session
type Session struct {
	ID              string
	ClaudeUUID      string // The .jsonl filename
	CWD             string
	State           SessionState
	LastActivity    time.Time
	ConnectedClients int
	Streaming       StreamingState // Current streaming state

	mu sync.RWMutex
}

// NewSession creates a new session
func NewSession(id string) *Session {
	return &Session{
		ID:           id,
		State:        StateIdle,
		LastActivity: time.Now(),
		CWD:          config.Get().WorkDir,
	}
}

// UpdateActivity updates the last activity time
func (s *Session) UpdateActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastActivity = time.Now()
}

// GetState returns the current state
func (s *Session) GetState() SessionState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State
}

// SetState sets the session state
func (s *Session) SetState(state SessionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.State = state
}

// SetClaudeUUID sets the Claude session UUID
func (s *Session) SetClaudeUUID(uuid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ClaudeUUID = uuid
}

// IncrementClients increments the connected client count
func (s *Session) IncrementClients() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ConnectedClients++
}

// DecrementClients decrements the connected client count
func (s *Session) DecrementClients() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ConnectedClients > 0 {
		s.ConnectedClients--
	}
}

// GetClientCount returns the number of connected clients
func (s *Session) GetClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ConnectedClients
}

// SetGenerating sets whether Claude is currently generating
func (s *Session) SetGenerating(generating bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Streaming.IsGenerating = generating
	if !generating {
		// Clear active blocks when generation completes
		s.Streaming.ActiveContentBlocks = nil
	}
}

// IsGenerating returns whether Claude is currently generating
func (s *Session) IsGenerating() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Streaming.IsGenerating
}

// AddContentBlock adds a content block to the streaming state
func (s *Session) AddContentBlock(block json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Streaming.ActiveContentBlocks = append(s.Streaming.ActiveContentBlocks, block)
}

// ClearContentBlocks clears all active content blocks
func (s *Session) ClearContentBlocks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Streaming.ActiveContentBlocks = nil
}

// GetStreamingState returns a copy of the current streaming state
func (s *Session) GetStreamingState() StreamingState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Make a copy to avoid race conditions
	blocks := make([]json.RawMessage, len(s.Streaming.ActiveContentBlocks))
	copy(blocks, s.Streaming.ActiveContentBlocks)

	return StreamingState{
		ActiveContentBlocks: blocks,
		IsGenerating:        s.Streaming.IsGenerating,
	}
}
