package internal

import (
	"context"
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// step.ws_spectator_mode_switch — updates the observation perspective for an existing
// spectator connection mid-game.
//
// Config: connectionId (string), gameId (string), mode (string), perspectivePlayerId (string)
// Output: mode (string), perspectivePlayerId (string), gameId (string)
type wsSpectatorModeSwitchStep struct {
	name                string
	connID              string
	gameID              string
	newMode             string
	perspectivePlayerID string
}

func newWSSpectatorModeSwitchStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &wsSpectatorModeSwitchStep{
		name:                name,
		connID:              stringFromMap(config, "connectionId"),
		gameID:              stringFromMap(config, "gameId"),
		newMode:             stringFromMapDefault(config, "mode", "anonymous"),
		perspectivePlayerID: stringFromMap(config, "perspectivePlayerId"),
	}, nil
}

func (s *wsSpectatorModeSwitchStep) Execute(_ context.Context, triggerData map[string]any,
	stepOutputs map[string]map[string]any, current map[string]any,
	metadata map[string]any, config map[string]any) (*sdk.StepResult, error) {

	h := GetHub()
	if h == nil {
		return &sdk.StepResult{Output: map[string]any{"error": "ws.server not initialized"}}, nil
	}

	connID := s.connID
	gameID := s.gameID
	mode := s.newMode
	pid := s.perspectivePlayerID
	if cfg, ok := config["connectionId"].(string); ok && cfg != "" {
		connID = cfg
	}
	if cfg, ok := config["gameId"].(string); ok && cfg != "" {
		gameID = cfg
	}
	if cfg, ok := config["mode"].(string); ok && cfg != "" {
		mode = cfg
	}
	if cfg, ok := config["perspectivePlayerId"].(string); ok && cfg != "" {
		pid = cfg
	}

	if mode == "player" && pid == "" {
		return nil, fmt.Errorf("step.ws_spectator_mode_switch: perspectivePlayerId required for player mode")
	}

	h.spectatorSetMode(connID, gameID, SpectatorMeta{
		Mode:                mode,
		PerspectivePlayerID: pid,
	})

	return &sdk.StepResult{Output: map[string]any{
		"mode":                mode,
		"perspectivePlayerId": pid,
		"gameId":              gameID,
	}}, nil
}

// stringFromMap extracts a string value from a map, returning "" if missing or not a string.
func stringFromMap(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

// stringFromMapDefault extracts a string value from a map with a default fallback.
func stringFromMapDefault(m map[string]any, key, def string) string {
	if m == nil {
		return def
	}
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return def
}
