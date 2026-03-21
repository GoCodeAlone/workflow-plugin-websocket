package internal

import (
	"context"

	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

type wsSendStep struct {
	name string
}

func newWSSendStep(name string, config map[string]any) (sdk.StepInstance, error) {
	return &wsSendStep{name: name}, nil
}

func (s *wsSendStep) Execute(ctx context.Context, triggerData map[string]any,
	stepOutputs map[string]map[string]any, current map[string]any,
	metadata map[string]any, config map[string]any) (*sdk.StepResult, error) {

	h := GetHub()
	if h == nil {
		return &sdk.StepResult{Output: map[string]any{"error": "ws.server not initialized", "sent": false}}, nil
	}

	connID, _ := config["connectionId"].(string)
	message, _ := config["message"].(string)

	if connID == "" {
		return &sdk.StepResult{Output: map[string]any{"error": "connectionId required", "sent": false}}, nil
	}

	var sent bool
	if hook := getSendHook(); hook != nil {
		sent = hook(connID, []byte(message))
	} else {
		sent = h.sendTo(connID, []byte(message))
	}
	return &sdk.StepResult{Output: map[string]any{"sent": sent}}, nil
}
