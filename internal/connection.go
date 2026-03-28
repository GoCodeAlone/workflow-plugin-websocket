package internal

import (
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type wsMessage struct {
	msgType int
	data    []byte
}

type connection struct {
	id     string
	conn   *websocket.Conn
	send   chan wsMessage
	hub    *hub
	mu     sync.Mutex
	closed bool
}

func (c *connection) readPump(onMessage func(connID string, msgType int, msg []byte)) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("ws readPump: panic", "connID", c.id, "panic", r)
		}
		callGlobalWSDisconnectHandler(c.id)
		c.hub.unregister <- c
		c.close()
	}()
	c.conn.SetReadLimit(c.hub.maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(c.hub.pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(c.hub.pongWait))
		return nil
	})
	for {
		msgType, msg, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
		if onMessage != nil {
			onMessage(c.id, msgType, msg)
		}
	}
}

func (c *connection) writePump() {
	ticker := time.NewTicker(c.hub.pingPeriod)
	defer func() {
		ticker.Stop()
		c.close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(msg.msgType, msg.data); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *connection) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		if c.conn != nil {
			c.conn.Close()
		}
		close(c.send)
	}
}
