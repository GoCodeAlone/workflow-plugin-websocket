package internal

import (
	"fmt"

	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

type wsPlugin struct{}

func NewWebSocketPlugin() sdk.PluginProvider {
	return &wsPlugin{}
}

func (p *wsPlugin) Manifest() sdk.PluginManifest {
	return sdk.PluginManifest{
		Name:        "workflow-plugin-websocket",
		Version:     "0.1.0",
		Author:      "GoCodeAlone",
		Description: "General-purpose WebSocket support for workflow applications",
	}
}

func (p *wsPlugin) ModuleTypes() []string {
	return []string{"ws.server"}
}

func (p *wsPlugin) StepTypes() []string {
	return []string{
		"step.ws_send",
		"step.ws_broadcast",
		"step.ws_room_join",
		"step.ws_room_leave",
		"step.ws_room_list",
		"step.ws_close",
	}
}

func (p *wsPlugin) CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error) {
	switch typeName {
	case "ws.server":
		return newWSServerModule(name, config)
	default:
		return nil, fmt.Errorf("unknown module type %q", typeName)
	}
}

func (p *wsPlugin) CreateStep(typeName, name string, config map[string]any) (sdk.StepInstance, error) {
	switch typeName {
	case "step.ws_send":
		return newWSSendStep(name, config)
	case "step.ws_broadcast":
		return newWSBroadcastStep(name, config)
	case "step.ws_room_join":
		return newWSRoomJoinStep(name, config)
	case "step.ws_room_leave":
		return newWSRoomLeaveStep(name, config)
	case "step.ws_room_list":
		return newWSRoomListStep(name, config)
	case "step.ws_close":
		return newWSCloseStep(name, config)
	default:
		return nil, fmt.Errorf("unknown step type %q", typeName)
	}
}
