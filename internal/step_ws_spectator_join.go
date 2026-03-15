package internal

import (
	"context"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// step.ws_spectator_join — joins a connection to the spectator room for a game,
// registering the observation mode. Adds the connection to spectator:<gameId> room.
//
// Config: connectionId (string), gameId (string), mode (string, default: "anonymous")
// Output: room (string), spectatorCount (int), mode (string)
type wsSpectatorJoinStep struct {
	name   string
	connID string
	gameID string
	mode   string
}

func newWSSpectatorJoinStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &wsSpectatorJoinStep{
		name:   name,
		connID: stringFromMap(config, "connectionId"),
		gameID: stringFromMap(config, "gameId"),
		mode:   stringFromMapDefault(config, "mode", "anonymous"),
	}, nil
}

func (s *wsSpectatorJoinStep) Execute(_ context.Context, triggerData map[string]any,
	stepOutputs map[string]map[string]any, current map[string]any,
	metadata map[string]any, config map[string]any) (*sdk.StepResult, error) {

	h := GetHub()
	if h == nil {
		return &sdk.StepResult{Output: map[string]any{
			"error": "ws.server not initialized", "spectatorCount": 0,
		}}, nil
	}

	// Resolve from config override or use stored values
	connID := s.connID
	gameID := s.gameID
	mode := s.mode
	if cfg, ok := config["connectionId"].(string); ok && cfg != "" {
		connID = cfg
	}
	if cfg, ok := config["gameId"].(string); ok && cfg != "" {
		gameID = cfg
	}
	if cfg, ok := config["mode"].(string); ok && cfg != "" {
		mode = cfg
	}

	h.spectatorJoin(connID, gameID, SpectatorMeta{Mode: mode})
	count := h.spectatorCount(gameID)

	return &sdk.StepResult{Output: map[string]any{
		"room":           "spectator:" + gameID,
		"spectatorCount": count,
		"mode":           mode,
		"gameId":         gameID,
	}}, nil
}
