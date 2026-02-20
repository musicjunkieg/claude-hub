package config

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Config holds the runtime configuration for claude-hub.
type Config struct {
	// HomeDir is the user's home directory.
	HomeDir string

	// WorkDir is the working directory where Claude processes are spawned.
	WorkDir string

	// ClaudeProjectsDir is the directory where Claude stores session .jsonl files.
	// Computed from WorkDir unless explicitly overridden via CLAUDE_PROJECTS_DIR.
	ClaudeProjectsDir string
}

var (
	globalConfig *Config
	once         sync.Once
)

// Get returns the global configuration, initializing it on first call.
func Get() *Config {
	once.Do(func() {
		globalConfig = load()
	})
	return globalConfig
}

// load reads configuration from environment variables with sensible defaults.
func load() *Config {
	homeDir := os.Getenv("HOME")
	if homeDir == "" {
		homeDir = "/home/sprite"
	}

	workDir := os.Getenv("CLAUDE_WORK_DIR")
	if workDir == "" {
		workDir = homeDir
	}

	projectsDir := os.Getenv("CLAUDE_PROJECTS_DIR")
	if projectsDir == "" {
		projectsDir = filepath.Join(homeDir, ".claude", "projects", encodePath(workDir))
	}

	cfg := &Config{
		HomeDir:           homeDir,
		WorkDir:           workDir,
		ClaudeProjectsDir: projectsDir,
	}

	log.Printf("Config: HomeDir=%s WorkDir=%s ClaudeProjectsDir=%s",
		cfg.HomeDir, cfg.WorkDir, cfg.ClaudeProjectsDir)

	return cfg
}

// encodePath converts a filesystem path to the format Claude uses for its
// projects directory names. Claude replaces each "/" with "-".
// For example, "/home/sprite" becomes "-home-sprite".
func encodePath(path string) string {
	return strings.ReplaceAll(path, "/", "-")
}
