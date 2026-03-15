package internal

import "sync"

// SpectatorMeta holds per-connection spectator configuration.
type SpectatorMeta struct {
	Mode                string // "anonymous" | "player" | "omniscient"
	PerspectivePlayerID string // only relevant for "player" mode
}

// spectatorRegistry tracks which connections are spectating which games
// and with what observation mode.
type spectatorRegistry struct {
	mu    sync.RWMutex
	// gameID -> connID -> meta
	games map[string]map[string]SpectatorMeta
	// connID -> set of gameIDs (used for cleanup on disconnect)
	conns map[string]map[string]bool
}

func newSpectatorRegistry() *spectatorRegistry {
	return &spectatorRegistry{
		games: make(map[string]map[string]SpectatorMeta),
		conns: make(map[string]map[string]bool),
	}
}

func (r *spectatorRegistry) join(connID, gameID string, meta SpectatorMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.games[gameID]; !ok {
		r.games[gameID] = make(map[string]SpectatorMeta)
	}
	r.games[gameID][connID] = meta
	if _, ok := r.conns[connID]; !ok {
		r.conns[connID] = make(map[string]bool)
	}
	r.conns[connID][gameID] = true
}

func (r *spectatorRegistry) setMode(connID, gameID string, meta SpectatorMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if game, ok := r.games[gameID]; ok {
		game[connID] = meta
	}
}

func (r *spectatorRegistry) leave(connID, gameID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if game, ok := r.games[gameID]; ok {
		delete(game, connID)
		if len(game) == 0 {
			delete(r.games, gameID)
		}
	}
	if conn, ok := r.conns[connID]; ok {
		delete(conn, gameID)
	}
}

func (r *spectatorRegistry) getMeta(connID, gameID string) (SpectatorMeta, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if game, ok := r.games[gameID]; ok {
		meta, ok := game[connID]
		return meta, ok
	}
	return SpectatorMeta{}, false
}

func (r *spectatorRegistry) count(gameID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.games[gameID])
}

func (r *spectatorRegistry) allForGame(gameID string) map[string]SpectatorMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]SpectatorMeta, len(r.games[gameID]))
	for k, v := range r.games[gameID] {
		out[k] = v
	}
	return out
}

// cleanupConn removes a connection from all spectator registrations.
// Called when a connection disconnects.
func (r *spectatorRegistry) cleanupConn(connID string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	gameIDs := make([]string, 0)
	if games, ok := r.conns[connID]; ok {
		for gameID := range games {
			gameIDs = append(gameIDs, gameID)
			if game, ok := r.games[gameID]; ok {
				delete(game, connID)
				if len(game) == 0 {
					delete(r.games, gameID)
				}
			}
		}
		delete(r.conns, connID)
	}
	return gameIDs
}
