package internal

import (
	"context"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// step.ws_spectator_leave — removes a connection from the spectator room for a game.
//
// Config: connectionId (string), gameId (string)
// Output: spectatorCount (int), gameId (string)
type wsSpectatorLeaveStep struct {
	name   string
	connID string
	gameID string
}

func newWSSpectatorLeaveStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &wsSpectatorLeaveStep{
		name:   name,
		connID: stringFromMap(config, "connectionId"),
		gameID: stringFromMap(config, "gameId"),
	}, nil
}

func (s *wsSpectatorLeaveStep) Execute(_ context.Context, triggerData map[string]any,
	stepOutputs map[string]map[string]any, current map[string]any,
	metadata map[string]any, config map[string]any) (*sdk.StepResult, error) {

	h := GetHub()
	if h == nil {
		return &sdk.StepResult{Output: map[string]any{
			"error": "ws.server not initialized", "spectatorCount": 0,
		}}, nil
	}

	connID := s.connID
	gameID := s.gameID
	if cfg, ok := config["connectionId"].(string); ok && cfg != "" {
		connID = cfg
	}
	if cfg, ok := config["gameId"].(string); ok && cfg != "" {
		gameID = cfg
	}

	h.spectatorLeave(connID, gameID)
	count := h.spectatorCount(gameID)

	return &sdk.StepResult{Output: map[string]any{
		"spectatorCount": count,
		"gameId":         gameID,
	}}, nil
}
