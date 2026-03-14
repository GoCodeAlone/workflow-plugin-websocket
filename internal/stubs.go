package internal

// stubs.go provides compile-time stubs. Replaced by full implementations in later tasks.

import (
	"context"
	"fmt"

	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// --- step stubs ---

type wsStepStub struct{ name string }

func (s *wsStepStub) Execute(_ context.Context, _ map[string]any, _ map[string]map[string]any, _ map[string]any, _ map[string]any, _ map[string]any) (*sdk.StepResult, error) {
	return nil, fmt.Errorf("step %s: not yet implemented", s.name)
}

func newWSSendStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &wsStepStub{name: name}, nil
}

func newWSBroadcastStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &wsStepStub{name: name}, nil
}

func newWSRoomJoinStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &wsStepStub{name: name}, nil
}

func newWSRoomLeaveStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &wsStepStub{name: name}, nil
}

func newWSRoomListStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &wsStepStub{name: name}, nil
}

func newWSCloseStep(name string, _ map[string]any) (sdk.StepInstance, error) {
	return &wsStepStub{name: name}, nil
}
