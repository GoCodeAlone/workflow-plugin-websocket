package internal

import (
	"context"

	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

type wsCloseStep struct{ name string }

func newWSCloseStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &wsCloseStep{name: name}, nil
}

func (s *wsCloseStep) Execute(ctx context.Context, triggerData map[string]any,
	stepOutputs map[string]map[string]any, current map[string]any,
	metadata map[string]any, config map[string]any) (*sdk.StepResult, error) {

	h := GetHub()
	if h == nil {
		return &sdk.StepResult{Output: map[string]any{"error": "ws.server not initialized", "closed": false}}, nil
	}

	connID, _ := current["connectionId"].(string)

	closed := h.closeConnection(connID)
	return &sdk.StepResult{Output: map[string]any{"closed": closed}}, nil
}
