package internal

import (
	websocketv1 "github.com/GoCodeAlone/workflow-plugin-websocket/gen"
	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// ContractRegistry returns the typed contract descriptors for all websocket
// module, step, and trigger types. The workflow engine calls this via the
// sdk.ContractProvider interface for strict validation.
func (p *wsPlugin) ContractRegistry() *pb.ContractRegistry {
	return websocketContractRegistry
}

// wsProtoPkg is the proto package prefix for all websocket typed messages.
const wsProtoPkg = "workflow.plugin.websocket.v1."

// websocketContractRegistry declares STRICT_PROTO contracts for the ws.server
// module, step.ws_send, step.ws_close, and the websocket trigger type.
var websocketContractRegistry = &pb.ContractRegistry{
	FileDescriptorSet: &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(structpb.File_google_protobuf_struct_proto),
			protodesc.ToFileDescriptorProto(websocketv1.File_websocket_proto),
		},
	},
	Contracts: []*pb.ContractDescriptor{
		// ── module ───────────────────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
			ModuleType:    "ws.server",
			ConfigMessage: wsProtoPkg + "WSServerConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		// ── step.ws_send ─────────────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.ws_send",
			ConfigMessage: wsProtoPkg + "WSSendConfig",
			InputMessage:  wsProtoPkg + "WSSendInput",
			OutputMessage: wsProtoPkg + "WSSendOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		// ── step.ws_close ────────────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.ws_close",
			ConfigMessage: wsProtoPkg + "WSCloseConfig",
			InputMessage:  wsProtoPkg + "WSCloseInput",
			OutputMessage: wsProtoPkg + "WSCloseOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		// ── trigger: websocket ───────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_TRIGGER,
			TriggerType:   "websocket",
			ConfigMessage: wsProtoPkg + "WebSocketTriggerConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
	},
}

// Compile-time assertion: wsPlugin implements sdk.ContractProvider.
var _ interface{ ContractRegistry() *pb.ContractRegistry } = (*wsPlugin)(nil)
