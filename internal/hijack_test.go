package internal

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

// nonHijackableWriter wraps http.ResponseWriter without implementing http.Hijacker.
// This simulates what happens when the workflow engine's trackedResponseWriter
// does NOT delegate Hijack() — the gorilla upgrader can't take over the TCP
// connection and the WebSocket upgrade fails with HTTP 500.
type nonHijackableWriter struct {
	http.ResponseWriter
}

// hijackableWriter wraps http.ResponseWriter and correctly delegates Hijack()
// to the underlying ResponseWriter (which, for real HTTP servers, implements
// http.Hijacker). This is the fix: trackedResponseWriter must implement
// http.Hijacker for WebSocket upgrades to work.
type hijackableWriter struct {
	http.ResponseWriter
}

func (h *hijackableWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return h.ResponseWriter.(http.Hijacker).Hijack()
}

// TestUpgradeWithWrappedResponseWriter verifies WebSocket upgrade works
// when ResponseWriter is wrapped (like workflow engine's trackedResponseWriter).
func TestUpgradeWithWrappedResponseWriter(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// --- Case 1: wrapper does NOT implement http.Hijacker → upgrade must fail ---
	t.Run("without_hijacker_fails", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			wrapped := &nonHijackableWriter{ResponseWriter: w}
			_, err := upgrader.Upgrade(wrapped, r, nil)
			if err == nil {
				t.Error("expected upgrade to fail with non-hijackable writer, but it succeeded")
			}
		}))
		defer srv.Close()

		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
		_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err == nil {
			t.Fatal("expected dial to fail, but it succeeded")
		}
		if resp != nil && resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", resp.StatusCode)
		}
	})

	// --- Case 2: wrapper DOES implement http.Hijacker → upgrade must succeed ---
	t.Run("with_hijacker_succeeds", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			wrapped := &hijackableWriter{ResponseWriter: w}
			conn, err := upgrader.Upgrade(wrapped, r, nil)
			if err != nil {
				t.Errorf("expected upgrade to succeed with hijackable writer, got error: %v", err)
				return
			}
			defer conn.Close()
			// Echo one message to prove the connection works.
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			_ = conn.WriteMessage(mt, msg)
		}))
		defer srv.Close()

		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
		conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("expected dial to succeed, got error: %v", err)
		}
		defer conn.Close()

		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Errorf("expected status 101, got %d", resp.StatusCode)
		}

		// Verify echo works.
		testMsg := []byte("hello")
		if err := conn.WriteMessage(websocket.TextMessage, testMsg); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		_, got, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if string(got) != string(testMsg) {
			t.Errorf("expected echo %q, got %q", testMsg, got)
		}
	})
}
