package workflowpluginwebsocket_test

import (
	"testing"

	"github.com/GoCodeAlone/workflow/wftest"
)

func TestWebSocket_SendMessagePipeline(t *testing.T) {
	sendRec := wftest.RecordStep("step.ws_send")
	sendRec.WithOutput(map[string]any{"sent": true})

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  ws-send:
    trigger:
      type: manual
    steps:
      - name: send
        type: step.ws_send
        config:
          connectionId: "conn-123"
          message: "hello"
`), sendRec)

	result := h.ExecutePipeline("ws-send", map[string]any{
		"connection_id": "conn-123",
		"message":       "hello",
	})
	if result.Error != nil {
		t.Fatalf("pipeline failed: %v", result.Error)
	}
	if sendRec.CallCount() != 1 {
		t.Errorf("expected 1 call to step.ws_send, got %d", sendRec.CallCount())
	}
	calls := sendRec.Calls()
	if calls[0].Config["connectionId"] != "conn-123" {
		t.Errorf("expected connectionId=conn-123 in config, got %v", calls[0].Config["connectionId"])
	}
}

func TestWebSocket_CloseConnectionPipeline(t *testing.T) {
	closeRec := wftest.RecordStep("step.ws_close")
	closeRec.WithOutput(map[string]any{"closed": true})

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  ws-close:
    trigger:
      type: manual
    steps:
      - name: close
        type: step.ws_close
        config:
          connectionId: "conn-456"
`), closeRec)

	result := h.ExecutePipeline("ws-close", map[string]any{
		"connection_id": "conn-456",
	})
	if result.Error != nil {
		t.Fatalf("pipeline failed: %v", result.Error)
	}
	if closeRec.CallCount() != 1 {
		t.Errorf("expected 1 call to step.ws_close, got %d", closeRec.CallCount())
	}
	calls := closeRec.Calls()
	if calls[0].Config["connectionId"] != "conn-456" {
		t.Errorf("expected connectionId=conn-456 in config, got %v", calls[0].Config["connectionId"])
	}
}

func TestWebSocket_SendThenClosePipeline(t *testing.T) {
	sendRec := wftest.RecordStep("step.ws_send")
	sendRec.WithOutput(map[string]any{"sent": true})
	closeRec := wftest.RecordStep("step.ws_close")
	closeRec.WithOutput(map[string]any{"closed": true})

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  ws-send-then-close:
    trigger:
      type: manual
    steps:
      - name: send
        type: step.ws_send
        config:
          connectionId: "conn-789"
          message: "goodbye"
      - name: close
        type: step.ws_close
        config:
          connectionId: "conn-789"
`), sendRec, closeRec)

	result := h.ExecutePipeline("ws-send-then-close", map[string]any{
		"connection_id": "conn-789",
	})
	if result.Error != nil {
		t.Fatalf("pipeline failed: %v", result.Error)
	}
	if sendRec.CallCount() != 1 {
		t.Errorf("expected 1 call to step.ws_send, got %d", sendRec.CallCount())
	}
	if closeRec.CallCount() != 1 {
		t.Errorf("expected 1 call to step.ws_close, got %d", closeRec.CallCount())
	}
	sendCalls := sendRec.Calls()
	if sendCalls[0].Config["message"] != "goodbye" {
		t.Errorf("expected message=goodbye in send config, got %v", sendCalls[0].Config["message"])
	}
}
