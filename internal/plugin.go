package internal

import (
	"fmt"
	"net/http"

	"github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// Version is set at build time via -ldflags
// "-X github.com/GoCodeAlone/workflow-plugin-websocket/internal.Version=X.Y.Z"
var Version = "0.0.0"

// WSServerInstance is the public interface for the ws.server module,
// exposing the HTTP handler methods needed for in-process registration.
type WSServerInstance interface {
	sdk.ModuleInstance
	ServeHTTP(w http.ResponseWriter, r *http.Request)
	HTTPPath() string
}

// NewWSServerModule creates a new ws.server module instance and returns it
// via the WSServerInstance interface so the host binary can register the
// WebSocket upgrade handler directly on the engine's HTTP router.
func NewWSServerModule(name string, config map[string]any) (WSServerInstance, error) {
	inst, err := newWSServerModule(name, config)
	if err != nil {
		return nil, err
	}
	return inst.(*wsServerModule), nil
}

type wsPlugin struct{}

func NewWebSocketPlugin() sdk.PluginProvider {
	return &wsPlugin{}
}

func (p *wsPlugin) Manifest() sdk.PluginManifest {
	return sdk.PluginManifest{
		Name:        "workflow-plugin-websocket",
		Version:     Version,
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
		"step.ws_close",
	}
}

func (p *wsPlugin) CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error) {
	switch typeName {
	case "ws.server":
		return newWSServerModule(name, config)
	case "websocket":
		// Trigger modules are created via CreateModule by the host's TriggerFactories.
		// No callback is available in this path; the trigger fires via the global handler.
		return newWSTrigger(config, nil)
	default:
		return nil, fmt.Errorf("unknown module type %q", typeName)
	}
}

func (p *wsPlugin) TriggerTypes() []string {
	return []string{"websocket"}
}

func (p *wsPlugin) CreateTrigger(typeName string, config map[string]any, cb sdk.TriggerCallback) (sdk.TriggerInstance, error) {
	switch typeName {
	case "websocket":
		inst, err := newWSTrigger(config, cb)
		if err != nil {
			return nil, err
		}
		return inst.(*wsTrigger), nil
	default:
		return nil, fmt.Errorf("unknown trigger type %q", typeName)
	}
}

func (p *wsPlugin) CreateStep(typeName, name string, config map[string]any) (sdk.StepInstance, error) {
	switch typeName {
	case "step.ws_send":
		return newWSSendStep(name, config)
	case "step.ws_close":
		return newWSCloseStep(name, config)
	default:
		return nil, fmt.Errorf("unknown step type %q", typeName)
	}
}
