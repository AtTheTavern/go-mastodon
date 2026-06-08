package mastodon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// DefaultWSReadTimeout is the time allowed without receiving any WebSocket
// frame (including Pong responses) before the connection is considered dead.
// Mastodon servers send Ping frames approximately every 30 seconds, so 90s
// allows for 2-3 missed Pongs before detecting a dead connection.
var DefaultWSReadTimeout = 90 * time.Second

// WSClient is a WebSocket client.
type WSClient struct {
	websocket.Dialer
	client *Client
}

// NewWSClient return WebSocket client.
func (c *Client) NewWSClient() *WSClient { return &WSClient{client: c} }

// Stream is a struct of data that flows in streaming.
type Stream struct {
	Event   string      `json:"event"`
	Payload interface{} `json:"payload"`
}

// StreamingWSUser return channel to read events on home using WebSocket.
func (c *WSClient) StreamingWSUser(ctx context.Context) (chan Event, error) {
	return c.streamingWS(ctx, "user", "")
}

// StreamingWSPublic return channel to read events on public using WebSocket.
func (c *WSClient) StreamingWSPublic(ctx context.Context, isLocal bool) (chan Event, error) {
	s := "public"
	if isLocal {
		s += ":local"
	}

	return c.streamingWS(ctx, s, "")
}

// StreamingWSHashtag return channel to read events on tagged timeline using WebSocket.
func (c *WSClient) StreamingWSHashtag(ctx context.Context, tag string, isLocal bool) (chan Event, error) {
	s := "hashtag"
	if isLocal {
		s += ":local"
	}

	return c.streamingWS(ctx, s, tag)
}

// StreamingWSList return channel to read events on a list using WebSocket.
func (c *WSClient) StreamingWSList(ctx context.Context, id ID) (chan Event, error) {
	return c.streamingWS(ctx, "list", string(id))
}

// StreamingWSDirect return channel to read events on a direct messages using WebSocket.
func (c *WSClient) StreamingWSDirect(ctx context.Context) (chan Event, error) {
	return c.streamingWS(ctx, "direct", "")
}

func (c *WSClient) streamingWS(ctx context.Context, stream, tag string) (chan Event, error) {
	params := url.Values{}
	params.Set("access_token", c.client.Config.AccessToken)
	params.Set("stream", stream)
	if tag != "" {
		params.Set("tag", tag)
	}

	u, err := changeWebSocketScheme(c.client.Config.Server)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, "/api/v1/streaming")
	u.RawQuery = params.Encode()

	q := make(chan Event)
	go func() {
		defer close(q)
		for {
			err := c.handleWS(ctx, u.String(), q)
			if err != nil {
				return
			}
		}
	}()

	return q, nil
}

func (c *WSClient) handleWS(ctx context.Context, rawurl string, q chan Event) error {
	conn, err := c.dialRedirect(rawurl)
	if err != nil {
		q <- &ErrorEvent{Err: err}

		// End.
		return err
	}

	readTimeout := DefaultWSReadTimeout

	// Set an initial read deadline so that a dead connection is detected
	// even if no data frames are received.
	conn.SetReadDeadline(time.Now().Add(readTimeout))

	// Set a PongHandler that resets the read deadline each time a Pong is
	// received. Mastodon servers send Ping frames approximately every 30s;
	// gorilla/websocket auto-responds with Pong, so the server's Ping and
	// our Pong response keep the connection alive at the protocol level.
	conn.SetPongHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	// Close the WebSocket when the context is canceled.
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			q <- &ErrorEvent{Err: ctx.Err()}

			// End.
			return ctx.Err()
		default:
		}

		var s Stream
		err := conn.ReadJSON(&s)
		if err != nil {
			q <- &ErrorEvent{Err: err}

			// Reconnect.
			break
		}

		// Reset read deadline after each successful read.
		conn.SetReadDeadline(time.Now().Add(readTimeout))

		err = nil
		switch s.Event {
		case "update":
			var status Status
			err = json.Unmarshal([]byte(s.Payload.(string)), &status)
			if err == nil {
				q <- &UpdateEvent{Status: &status}
			}
		case "status.update":
			var status Status
			err = json.Unmarshal([]byte(s.Payload.(string)), &status)
			if err == nil {
				q <- &UpdateEditEvent{Status: &status}
			}
		case "notification":
			var notification Notification
			err = json.Unmarshal([]byte(s.Payload.(string)), &notification)
			if err == nil {
				q <- &NotificationEvent{Notification: &notification}
			}
		case "conversation":
			var conversation Conversation
			err = json.Unmarshal([]byte(s.Payload.(string)), &conversation)
			if err == nil {
				q <- &ConversationEvent{Conversation: &conversation}
			}
		case "delete":
			if f, ok := s.Payload.(float64); ok {
				q <- &DeleteEvent{ID: ID(fmt.Sprint(int64(f)))}
			} else {
				q <- &DeleteEvent{ID: ID(strings.TrimSpace(s.Payload.(string)))}
			}
		}
		if err != nil {
			q <- &ErrorEvent{err}
		}
	}

	return nil
}

func (c *WSClient) dialRedirect(rawurl string) (conn *websocket.Conn, err error) {
	for {
		conn, rawurl, err = c.dial(rawurl)
		if err != nil {
			return nil, err
		} else if conn != nil {
			return conn, nil
		}
	}
}

func (c *WSClient) dial(rawurl string) (*websocket.Conn, string, error) {
	conn, resp, err := c.Dial(rawurl, nil)
	if err != nil && err != websocket.ErrBadHandshake {
		return nil, "", err
	}
	defer resp.Body.Close()

	if loc := resp.Header.Get("Location"); loc != "" {
		u, err := changeWebSocketScheme(loc)
		if err != nil {
			return nil, "", err
		}

		return nil, u.String(), nil
	}

	return conn, "", err
}

func changeWebSocketScheme(rawurl string) (*url.URL, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}

	return u, nil
}

