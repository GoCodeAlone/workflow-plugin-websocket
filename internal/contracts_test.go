package internal

import (
	"testing"

	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
)

func TestContractRegistry_HasAllSurfaces(t *testing.T) {
	p := &wsPlugin{}
	reg := p.ContractRegistry()

	if reg == nil {
		t.Fatal("ContractRegistry() returned nil")
	}

	// Expect 4 contracts: 1 module + 2 steps + 1 trigger
	if len(reg.Contracts) != 4 {
		t.Fatalf("expected 4 contracts, got %d", len(reg.Contracts))
	}

	type want struct {
		kind       pb.ContractKind
		moduleType string
		stepType   string
		triggerType string
	}
	wantContracts := []want{
		{kind: pb.ContractKind_CONTRACT_KIND_MODULE, moduleType: "ws.server"},
		{kind: pb.ContractKind_CONTRACT_KIND_STEP, stepType: "step.ws_send"},
		{kind: pb.ContractKind_CONTRACT_KIND_STEP, stepType: "step.ws_close"},
		{kind: pb.ContractKind_CONTRACT_KIND_TRIGGER, triggerType: "websocket"},
	}

	for i, w := range wantContracts {
		c := reg.Contracts[i]
		if c.Kind != w.kind {
			t.Errorf("contract[%d]: want kind %v, got %v", i, w.kind, c.Kind)
		}
		if c.Mode != pb.ContractMode_CONTRACT_MODE_STRICT_PROTO {
			t.Errorf("contract[%d]: want STRICT_PROTO mode, got %v", i, c.Mode)
		}
		if w.moduleType != "" && c.ModuleType != w.moduleType {
			t.Errorf("contract[%d]: want module_type %q, got %q", i, w.moduleType, c.ModuleType)
		}
		if w.stepType != "" && c.StepType != w.stepType {
			t.Errorf("contract[%d]: want step_type %q, got %q", i, w.stepType, c.StepType)
		}
		if w.triggerType != "" && c.TriggerType != w.triggerType {
			t.Errorf("contract[%d]: want trigger_type %q, got %q", i, w.triggerType, c.TriggerType)
		}
	}
}

func TestContractRegistry_FileDescriptorSetIncludesWebsocketAndStruct(t *testing.T) {
	p := &wsPlugin{}
	reg := p.ContractRegistry()

	if reg.FileDescriptorSet == nil {
		t.Fatal("FileDescriptorSet is nil")
	}

	var foundWebsocket, foundStruct bool
	for _, fd := range reg.FileDescriptorSet.File {
		name := fd.GetName()
		if name == "websocket.proto" {
			foundWebsocket = true
		}
		if name == "google/protobuf/struct.proto" {
			foundStruct = true
		}
	}

	if !foundWebsocket {
		t.Error("FileDescriptorSet missing websocket.proto")
	}
	if !foundStruct {
		t.Error("FileDescriptorSet missing google/protobuf/struct.proto")
	}
}
