package hub

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"claude-hub/internal/config"
	"claude-hub/internal/process"
	"claude-hub/internal/session"
	"claude-hub/internal/watcher"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
)

// Hub maintains the set of active clients and broadcasts messages
type Hub struct {
	// Sessions maps session ID to Session
	sessions map[string]*session.Session

	// Clients maps session ID to set of clients
	clients map[string]map[*Client]bool

	// Process manager
	processMgr *process.Manager

	// Terminal detector
	detector *process.TerminalDetector

	// Watchers maps session ID to file watcher
	watchers map[string]*watcher.SessionWatcher

	// Claude projects directory path
	claudeProjectsDir string

	// Register requests from clients
	register chan *Client

	// Unregister requests from clients
	unregister chan *Client

	// Broadcast message to all clients in a session
	broadcast chan *BroadcastMessage

	mu sync.RWMutex
}

// BroadcastMessage represents a message to broadcast to a session
type BroadcastMessage struct {
	SessionID string
	Data      []byte
	Exclude   *Client // Optional: don't send to this client
}

// NewHub creates a new Hub
func NewHub() *Hub {
	cfg := config.Get()

	h := &Hub{
		sessions:          make(map[string]*session.Session),
		clients:           make(map[string]map[*Client]bool),
		processMgr:        process.NewManager(),
		detector:          process.NewTerminalDetector(),
		watchers:          make(map[string]*watcher.SessionWatcher),
		claudeProjectsDir: cfg.ClaudeProjectsDir,
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		broadcast:         make(chan *BroadcastMessage, 256),
	}

	// Start watching the Claude projects directory for file changes
	go h.watchProjectsDirectory()

	return h
}

// NewClient creates a new client
func (h *Hub) NewClient(conn *websocket.Conn, sessionID, clientID string) *Client {
	return &Client{
		hub:       h,
		conn:      conn,
		send:      make(chan []byte, 256),
		sessionID: sessionID,
		clientID:  clientID,
	}
}

// RegisterClient registers a client with the hub
func (h *Hub) RegisterClient(client *Client) {
	h.register <- client
}

// Run starts the hub's main loop
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.registerClient(client)

		case client := <-h.unregister:
			h.unregisterClient(client)

		case message := <-h.broadcast:
			h.broadcastToSession(message)
		}
	}
}

func (h *Hub) registerClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Create client set for session if it doesn't exist
	if h.clients[client.sessionID] == nil {
		h.clients[client.sessionID] = make(map[*Client]bool)
	}

	// Add client to session
	h.clients[client.sessionID][client] = true

	// Create session if it doesn't exist
	sess := h.sessions[client.sessionID]
	if sess == nil {
		sess = session.NewSession(client.sessionID)

		// Check if a Claude session file exists for this session ID
		// This handles the case where the user refreshed and the session was
		// synced to Claude's UUID, but claude-hub restarted or lost the session from memory
		claudeSessionFile := filepath.Join(h.claudeProjectsDir, client.sessionID+".jsonl")
		if _, err := os.Stat(claudeSessionFile); err == nil {
			// File exists! This is a resume scenario
			log.Printf("Created new session: %s (found existing Claude session file, will resume)", client.sessionID)
			sess.SetClaudeUUID(client.sessionID)
		} else {
			log.Printf("Created new session: %s", client.sessionID)
		}

		h.sessions[client.sessionID] = sess
	}

	sess.IncrementClients()

	log.Printf("Client %s connected to session %s (%d clients total)",
		client.clientID, client.sessionID, len(h.clients[client.sessionID]))

	// Check if Claude is currently generating
	isGenerating := false
	if hp, err := h.processMgr.Get(client.sessionID); err == nil {
		isGenerating = hp.IsGenerating
	}

	// Send history if we have a Claude UUID
	if sess.ClaudeUUID != "" {
		go h.sendHistoryToClient(client, sess.ClaudeUUID, isGenerating)
	}

	// Spawn Claude process if this is the first client and no process exists
	if sess.GetClientCount() == 1 && sess.GetState() == session.StateIdle {
		go h.spawnClaudeForSession(client.sessionID, sess)
	}

	// Send system message to new client
	systemMsg := map[string]interface{}{
		"type":      "system",
		"message":   "Connected to Claude Hub (Phase 3)",
		"sessionId": client.sessionID,
	}
	if data, err := json.Marshal(systemMsg); err == nil {
		select {
		case client.send <- data:
		default:
			close(client.send)
			delete(h.clients[client.sessionID], client)
		}
	}

	// If Claude is generating, send processing indicator to new client
	// This ensures they know to expect streaming content
	if isGenerating {
		processingMsg := map[string]interface{}{
			"type":         "processing",
			"isProcessing": true,
		}
		if data, err := json.Marshal(processingMsg); err == nil {
			select {
			case client.send <- data:
			default:
			}
		}
	}
}

func (h *Hub) unregisterClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[client.sessionID]; ok {
		if _, ok := h.clients[client.sessionID][client]; ok {
			delete(h.clients[client.sessionID], client)
			close(client.send)

			if sess := h.sessions[client.sessionID]; sess != nil {
				sess.DecrementClients()
			}

			log.Printf("Client %s disconnected from session %s (%d clients remaining)",
				client.clientID, client.sessionID, len(h.clients[client.sessionID]))

			// Clean up empty session (keep process running for now)
			if len(h.clients[client.sessionID]) == 0 {
				delete(h.clients, client.sessionID)
				log.Printf("No more clients for session %s (process still running)", client.sessionID)
			}
		}
	}
}

func (h *Hub) broadcastToSession(message *BroadcastMessage) {
	h.mu.RLock()
	clients := h.clients[message.SessionID]
	h.mu.RUnlock()

	for client := range clients {
		// Skip excluded client (e.g., the sender)
		if message.Exclude != nil && client == message.Exclude {
			continue
		}

		select {
		case client.send <- message.Data:
		default:
			// Client send buffer full, close it
			close(client.send)
			h.mu.Lock()
			delete(h.clients[message.SessionID], client)
			h.mu.Unlock()
		}
	}
}

// handleClientMessage processes messages from clients
func (h *Hub) handleClientMessage(client *Client, msg *ClientMessage) {
	log.Printf("[%s] Received %s message from client %s",
		client.sessionID, msg.Type, client.clientID)

	switch msg.Type {
	case "user":
		h.handleUserMessage(client, msg)

	case "interrupt":
		h.handleInterrupt(client)

	default:
		log.Printf("[%s] Unknown message type: %s", client.sessionID, msg.Type)
	}
}

// handleUserMessage sends a user message to Claude
func (h *Hub) handleUserMessage(client *Client, msg *ClientMessage) {
	// Broadcast to other clients (include all fields including image data)
	messageContent := map[string]interface{}{
		"role":    "user",
		"content": msg.Content,
	}

	// Include image fields if present
	if msg.ImageID != "" {
		messageContent["image"] = map[string]interface{}{
			"id":        msg.ImageID,
			"filename":  msg.ImageFilename,
			"mediaType": msg.ImageMediaType,
		}
	}

	userMsg := map[string]interface{}{
		"type":    "user_message",
		"message": messageContent,
	}

	data, _ := json.Marshal(userMsg)
	h.broadcast <- &BroadcastMessage{
		SessionID: client.sessionID,
		Data:      data,
		Exclude:   client,
	}

	// Check if we need to spawn a process first
	sess := h.GetSession(client.sessionID)
	if sess != nil && sess.GetState() == session.StateIdle {
		log.Printf("[%s] No process running, spawning before sending message", client.sessionID)
		h.spawnClaudeForSession(client.sessionID, sess)
		// Wait a bit for process to start
		time.Sleep(500 * time.Millisecond)
	}

	// Send to Claude process
	if err := h.processMgr.SendMessage(client.sessionID, msg.Content); err != nil {
		log.Printf("[%s] Error sending message to Claude: %v", client.sessionID, err)

		// Try spawning if no process exists
		if sess != nil {
			log.Printf("[%s] Attempting to spawn process and retry", client.sessionID)
			h.spawnClaudeForSession(client.sessionID, sess)
			time.Sleep(500 * time.Millisecond)

			// Retry send
			if err := h.processMgr.SendMessage(client.sessionID, msg.Content); err != nil {
				log.Printf("[%s] Retry failed: %v", client.sessionID, err)
				errMsg, _ := json.Marshal(map[string]interface{}{
					"type":    "error",
					"message": "Failed to send message to Claude: " + err.Error(),
				})
				select {
				case client.send <- errMsg:
				default:
				}
				return
			}
		}
	}

	// Send processing indicator
	processingMsg, _ := json.Marshal(map[string]interface{}{
		"type":         "processing",
		"isProcessing": true,
	})
	h.broadcast <- &BroadcastMessage{
		SessionID: client.sessionID,
		Data:      processingMsg,
	}
}

// handleInterrupt interrupts the current Claude generation
func (h *Hub) handleInterrupt(client *Client) {
	log.Printf("[%s] Interrupt received", client.sessionID)

	if err := h.processMgr.Kill(client.sessionID); err != nil {
		log.Printf("[%s] Error interrupting Claude: %v", client.sessionID, err)
	}

	// Send result to indicate processing stopped
	resultMsg, _ := json.Marshal(map[string]interface{}{
		"type": "result",
	})
	h.broadcast <- &BroadcastMessage{
		SessionID: client.sessionID,
		Data:      resultMsg,
	}

	// Respawn process immediately
	if sess := h.GetSession(client.sessionID); sess != nil {
		log.Printf("[%s] Respawning process after interrupt", client.sessionID)
		go h.spawnClaudeForSession(client.sessionID, sess)
	}
}

// GetSession returns a session by ID (for future use)
func (h *Hub) GetSession(sessionID string) *session.Session {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.sessions[sessionID]
}

// spawnClaudeForSession spawns a Claude process for a session
func (h *Hub) spawnClaudeForSession(sessionID string, sess *session.Session) {
	log.Printf("[%s] Spawning headless Claude process", sessionID)

	// Stop file watcher if it's running (transitioning from TERMINAL_ONLY → WEB_ONLY)
	h.stopFileWatching(sessionID)

	// For new sessions, pass empty claudeUUID to let Claude generate its own
	// For existing sessions, use the stored ClaudeUUID to resume
	hp, err := h.processMgr.Spawn(sessionID, sess.CWD, sess.ClaudeUUID)
	if err != nil {
		log.Printf("[%s] Failed to spawn Claude: %v", sessionID, err)
		errMsg, _ := json.Marshal(map[string]interface{}{
			"type":    "error",
			"message": "Failed to start Claude process",
		})
		h.broadcast <- &BroadcastMessage{
			SessionID: sessionID,
			Data:      errMsg,
		}
		return
	}

	sess.SetState(session.StateWebOnly)

	// Handle output from Claude
	go h.handleClaudeOutput(sessionID, hp)
}

// handleClaudeOutput reads output from Claude and broadcasts to clients
func (h *Hub) handleClaudeOutput(sessionID string, hp *process.HeadlessProcess) {
	sess := h.GetSession(sessionID)

	// Register this PID with the detector
	if hp.Cmd != nil && hp.Cmd.Process != nil {
		h.detector.RegisterOwnPID(hp.Cmd.Process.Pid)
		defer h.detector.UnregisterOwnPID(hp.Cmd.Process.Pid)
	}

	for {
		select {
		case msg, ok := <-hp.OutputChan:
			if !ok {
				// Channel closed, process terminated
				log.Printf("[%s] Claude process output channel closed", sessionID)
				return
			}

			// Update session with Claude UUID if this is an init message
			if msg.Type == "system" && msg.Subtype == "init" && msg.SessionID != "" {
				if sess != nil {
					sess.SetClaudeUUID(msg.SessionID)
					log.Printf("[%s] Updated Claude UUID: %s", sessionID, msg.SessionID)
				}
			}

			// Track streaming state
			if sess != nil {
				switch msg.Type {
				case "content_block_start":
					// Add this content block to streaming state
					if blockData, err := json.Marshal(msg); err == nil {
						sess.AddContentBlock(json.RawMessage(blockData))
						sess.SetGenerating(true)
					}
				case "message_stop", "result":
					// Clear streaming state when generation completes
					sess.ClearContentBlocks()
					sess.SetGenerating(false)
				}
			}

			// Marshal and broadcast to all clients
			data, err := json.Marshal(msg)
			if err != nil {
				log.Printf("[%s] Error marshaling Claude output: %v", sessionID, err)
				continue
			}

			h.broadcast <- &BroadcastMessage{
				SessionID: sessionID,
				Data:      data,
			}

		case err, ok := <-hp.ErrorChan:
			if !ok {
				return
			}
			log.Printf("[%s] Claude process error: %v", sessionID, err)
			errMsg, _ := json.Marshal(map[string]interface{}{
				"type":    "error",
				"message": err.Error(),
			})
			h.broadcast <- &BroadcastMessage{
				SessionID: sessionID,
				Data:      errMsg,
			}

		case <-hp.Done():
			log.Printf("[%s] Claude process context cancelled", sessionID)
			return
		}
	}
}

// watchProjectsDirectory watches the Claude projects directory for file changes
func (h *Hub) watchProjectsDirectory() {
	projectDir := h.claudeProjectsDir

	// Ensure directory exists
	if err := os.MkdirAll(projectDir, 0755); err != nil {
		log.Printf("Failed to create projects directory %s: %v", projectDir, err)
		return
	}

	// Create fsnotify watcher for the directory
	dirWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("Failed to create directory watcher: %v", err)
		return
	}
	defer dirWatcher.Close()

	if err := dirWatcher.Add(projectDir); err != nil {
		log.Printf("Failed to watch projects directory: %v", err)
		return
	}

	log.Printf("Watching projects directory: %s", projectDir)

	for {
		select {
		case event, ok := <-dirWatcher.Events:
			if !ok {
				return
			}

			// Only care about write events to .jsonl files
			if event.Op&fsnotify.Write == fsnotify.Write && strings.HasSuffix(event.Name, ".jsonl") {
				h.handleProjectFileChange(event.Name)
			}

		case err, ok := <-dirWatcher.Errors:
			if !ok {
				return
			}
			log.Printf("Directory watcher error: %v", err)
		}
	}
}

// handleProjectFileChange handles when a .jsonl file in the projects directory changes
func (h *Hub) handleProjectFileChange(filePath string) {
	// Extract Claude UUID from filename
	base := filepath.Base(filePath)
	claudeUUID := strings.TrimSuffix(base, ".jsonl")

	h.mu.Lock()
	defer h.mu.Unlock()

	// Find session by Claude UUID
	var sess *session.Session
	var sessionID string
	for id, s := range h.sessions {
		if s.ClaudeUUID == claudeUUID {
			sess = s
			sessionID = id
			break
		}
	}

	if sess == nil {
		// No session tracking this UUID, ignore silently
		return
	}

	log.Printf("File changed: %s -> session %s (state: %s)", base, sessionID, sess.GetState().String())

	// Check if we have a headless process for this session
	hp, err := h.processMgr.Get(sessionID)
	hasHeadlessProcess := (err == nil)

	log.Printf("  -> Has headless process: %v", hasHeadlessProcess)

	currentState := sess.GetState()

	// If we're in WEB_ONLY mode with a headless process, check if it's generating
	// If not generating and file changes, it's probably terminal activity
	if currentState == session.StateWebOnly && hasHeadlessProcess {
		if !hp.IsGenerating {
			log.Printf("[%s] File changed while headless not generating - terminal session likely", sessionID)

			// Kill headless process
			h.processMgr.Kill(sessionID)

			// Transition to terminal mode
			sess.TransitionTo(session.StateTerminalOnly)

			// Start file watching
			if h.watchers[sessionID] == nil {
				h.startFileWatchingUnlocked(sessionID, sess)
			}

			// Notify clients
			systemMsg, _ := json.Marshal(map[string]interface{}{
				"type":    "system",
				"message": "Terminal session detected - file watching active",
			})
			h.broadcast <- &BroadcastMessage{
				SessionID: sessionID,
				Data:      systemMsg,
			}
		}
	} else if !hasHeadlessProcess && currentState != session.StateTerminalOnly {
		log.Printf("[%s] File changed without headless process - terminal session detected", sessionID)

		// Transition to terminal mode
		sess.TransitionTo(session.StateTerminalOnly)

		// Start file watching
		if h.watchers[sessionID] == nil {
			h.startFileWatchingUnlocked(sessionID, sess)
		}

		// Notify clients
		systemMsg, _ := json.Marshal(map[string]interface{}{
			"type":    "system",
			"message": "Terminal session detected - file watching active",
		})
		h.broadcast <- &BroadcastMessage{
			SessionID: sessionID,
			Data:      systemMsg,
		}
	}
}

// startFileWatchingUnlocked starts watching without taking the lock (caller must hold lock)
func (h *Hub) startFileWatchingUnlocked(sessionID string, sess *session.Session) {
	// Don't start if already watching
	if h.watchers[sessionID] != nil {
		return
	}

	// Need Claude UUID to watch
	if sess.ClaudeUUID == "" {
		log.Printf("[%s] Cannot start watching: no Claude UUID", sessionID)
		return
	}

	w, err := watcher.NewSessionWatcher(sessionID, sess.ClaudeUUID)
	if err != nil {
		log.Printf("[%s] Failed to create watcher: %v", sessionID, err)
		return
	}

	h.watchers[sessionID] = w
	w.Start()

	// Handle watcher events (unlock first to avoid deadlock)
	go h.handleWatcherEvents(sessionID, w)
}

// scanForTerminalSessions scans for terminal processes and handles state transitions
func (h *Hub) scanForTerminalSessions() {
	processes, err := h.detector.ScanForTerminalSessions()
	if err != nil {
		log.Printf("Error scanning for terminal sessions: %v", err)
		return
	}

	// Debug: log detected processes
	if len(processes) > 0 {
		log.Printf("Detected %d terminal Claude processes", len(processes))
		for _, proc := range processes {
			log.Printf("  - PID %d, Session: %s", proc.PID, proc.SessionID)
		}
	}

	// Check each detected process
	for _, proc := range processes {
		h.handleTerminalDetected(proc.SessionID, proc.PID)
	}

	// Check for terminal processes that have exited
	h.checkTerminalExits()
}

// handleTerminalDetected handles when a terminal Claude process is detected
func (h *Hub) handleTerminalDetected(claudeUUID string, pid int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	log.Printf("Attempting to match terminal session UUID: %s", claudeUUID)

	// Debug: show all known sessions
	for id, s := range h.sessions {
		log.Printf("  Known session %s -> Claude UUID: %s (state: %s)",
			id, s.ClaudeUUID, s.GetState().String())
	}

	// Find session by Claude UUID
	var sess *session.Session
	var sessionID string
	for id, s := range h.sessions {
		if s.ClaudeUUID == claudeUUID {
			sess = s
			sessionID = id
			break
		}
	}

	if sess == nil {
		// Unknown session, ignore
		log.Printf("No matching session found for Claude UUID: %s", claudeUUID)
		return
	}

	// Check current state
	currentState := sess.GetState()

	if currentState == session.StateWebOnly {
		// Terminal process detected while in web-only mode
		// Transition: WEB_ONLY -> TRANSITIONING -> TERMINAL_ONLY
		log.Printf("[%s] Terminal process detected (PID: %d), killing headless", sessionID, pid)

		// Transition to TRANSITIONING
		sess.TransitionTo(session.StateTransitioning)

		// Kill headless process
		if err := h.processMgr.Kill(sessionID); err != nil {
			log.Printf("[%s] Error killing headless process: %v", sessionID, err)
		}

		// Transition to TERMINAL_ONLY
		sess.TransitionTo(session.StateTerminalOnly)

		// Start file watching
		h.startFileWatching(sessionID, sess)

		// Notify clients
		systemMsg, _ := json.Marshal(map[string]interface{}{
			"type":    "system",
			"message": "Switched to terminal mode - file watching active",
		})
		h.broadcast <- &BroadcastMessage{
			SessionID: sessionID,
			Data:      systemMsg,
		}
	}
}

// checkTerminalExits checks if terminal processes have exited
func (h *Hub) checkTerminalExits() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for sessionID, sess := range h.sessions {
		if sess.GetState() != session.StateTerminalOnly {
			continue
		}

		// Check if there's still a terminal process for this session
		proc, _ := h.detector.FindSessionProcess(sess.ClaudeUUID)
		if proc == nil {
			// Terminal process exited
			log.Printf("[%s] Terminal process exited, returning to web mode", sessionID)

			// Stop file watching
			h.stopFileWatching(sessionID)

			// Transition back to WEB_ONLY
			sess.TransitionTo(session.StateWebOnly)

			// Respawn headless if there are connected clients
			if sess.GetClientCount() > 0 {
				go h.spawnClaudeForSession(sessionID, sess)

				// Notify clients
				systemMsg, _ := json.Marshal(map[string]interface{}{
					"type":    "system",
					"message": "Terminal session ended - returning to web mode",
				})
				h.broadcast <- &BroadcastMessage{
					SessionID: sessionID,
					Data:      systemMsg,
				}
			}
		}
	}
}

// startFileWatching starts watching a session file
func (h *Hub) startFileWatching(sessionID string, sess *session.Session) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Don't start if already watching
	if h.watchers[sessionID] != nil {
		return
	}

	// Need Claude UUID to watch
	if sess.ClaudeUUID == "" {
		log.Printf("[%s] Cannot start watching: no Claude UUID", sessionID)
		return
	}

	w, err := watcher.NewSessionWatcher(sessionID, sess.ClaudeUUID)
	if err != nil {
		log.Printf("[%s] Failed to create watcher: %v", sessionID, err)
		return
	}

	h.watchers[sessionID] = w
	w.Start()

	// Handle watcher events
	go h.handleWatcherEvents(sessionID, w)
}

// stopFileWatching stops watching a session file
func (h *Hub) stopFileWatching(sessionID string) {
	h.mu.Lock()
	w := h.watchers[sessionID]
	delete(h.watchers, sessionID)
	h.mu.Unlock()

	if w != nil {
		w.Stop()
		log.Printf("[%s] Stopped file watching", sessionID)
	}
}

// handleWatcherEvents handles events from a file watcher
func (h *Hub) handleWatcherEvents(sessionID string, w *watcher.SessionWatcher) {
	for event := range w.Events() {
		if event.Role == "user" {
			// Send user message
			userMsg := map[string]interface{}{
				"type": "user_message",
				"message": map[string]interface{}{
					"role":      "user",
					"content":   event.Content,
					"timestamp": event.Timestamp.Unix() * 1000,
				},
			}
			data, _ := json.Marshal(userMsg)
			h.broadcast <- &BroadcastMessage{
				SessionID: sessionID,
				Data:      data,
			}
		} else if event.Role == "assistant" {
			// For assistant messages from file, send as complete message with result
			// This simulates what sprite-mobile does when saving messages
			assistantMsg := map[string]interface{}{
				"type": "assistant",
				"message": map[string]interface{}{
					"role":      "assistant",
					"content":   event.Content,
					"timestamp": event.Timestamp.Unix() * 1000,
				},
			}
			assistantData, _ := json.Marshal(assistantMsg)
			h.broadcast <- &BroadcastMessage{
				SessionID: sessionID,
				Data:      assistantData,
			}

			// Send result to mark completion
			resultMsg := map[string]interface{}{
				"type": "result",
			}
			resultData, _ := json.Marshal(resultMsg)
			h.broadcast <- &BroadcastMessage{
				SessionID: sessionID,
				Data:      resultData,
			}
		}
	}
}

// sendHistoryToClient sends message history from .jsonl file to a client
func (h *Hub) sendHistoryToClient(client *Client, claudeUUID string, isGenerating bool) {
	// Use watcher to parse the file
	w, err := watcher.NewSessionWatcher(client.sessionID, claudeUUID)
	if err != nil {
		log.Printf("[%s] Failed to load history: %v", client.sessionID, err)
		return
	}

	// Read the entire file from the beginning
	w.Offset = 0

	// Manually trigger a read to get all messages
	// We'll create a temporary slice to collect messages
	messages := []map[string]interface{}{}

	// This is a simplified version - just read and parse the whole file
	filePath := filepath.Join(h.claudeProjectsDir, claudeUUID+".jsonl")

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("[%s] Could not open session file for history: %v", client.sessionID, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		msg, err := watcher.ParseJSONLLine(line)
		if err != nil {
			continue
		}

		parsed, err := watcher.ExtractContent(msg)
		if err != nil || parsed == nil {
			continue
		}

		messages = append(messages, map[string]interface{}{
			"role":      parsed.Role,
			"content":   parsed.Content,
			"timestamp": parsed.Timestamp.Unix() * 1000, // ms
		})
	}

	if len(messages) > 0 {
		historyMsg := map[string]interface{}{
			"type":         "history",
			"messages":     messages,
			"isGenerating": isGenerating, // Include generation state
		}

		data, _ := json.Marshal(historyMsg)
		select {
		case client.send <- data:
			log.Printf("[%s] Sent %d messages from history to client (generating: %v)", client.sessionID, len(messages), isGenerating)
		default:
			log.Printf("[%s] Failed to send history - client send buffer full", client.sessionID)
		}
	} else if isGenerating {
		// Even if no history, send a message indicating generation is in progress
		historyMsg := map[string]interface{}{
			"type":         "history",
			"messages":     []map[string]interface{}{},
			"isGenerating": true,
		}
		data, _ := json.Marshal(historyMsg)
		select {
		case client.send <- data:
			log.Printf("[%s] Sent empty history with generating=true", client.sessionID)
		default:
		}
	}

	// Send active content blocks if Claude is currently generating
	// This allows reconnecting clients to catch up on streaming state
	if isGenerating {
		sess := h.GetSession(client.sessionID)
		if sess != nil {
			streamingState := sess.GetStreamingState()
			if len(streamingState.ActiveContentBlocks) > 0 {
				log.Printf("[%s] Sending %d active content blocks to reconnecting client", client.sessionID, len(streamingState.ActiveContentBlocks))
				for _, block := range streamingState.ActiveContentBlocks {
					select {
					case client.send <- block:
					default:
						log.Printf("[%s] Failed to send content block - client buffer full", client.sessionID)
					}
				}
			}
		}
	}
}

// GetHealthStatus returns health status including active generation info
func (h *Hub) GetHealthStatus() []byte {
	h.mu.RLock()
	defer h.mu.RUnlock()

	activeGenerating := h.processMgr.GetActiveGeneratingCount()
	totalProcesses := h.processMgr.GetProcessCount()
	totalSessions := len(h.sessions)
	activeConnections := 0

	for _, clients := range h.clients {
		activeConnections += len(clients)
	}

	status := map[string]interface{}{
		"status":              "ok",
		"active_sessions":     totalSessions,
		"active_processes":    totalProcesses,
		"generating":          activeGenerating,
		"active_connections":  activeConnections,
		"keep_sprite_awake":   activeGenerating > 0,
	}

	data, _ := json.Marshal(status)
	return data
}

