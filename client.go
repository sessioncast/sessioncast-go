package sessioncast

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultPingInterval = 30 * time.Second
	defaultTimeout      = 30 * time.Second
	writeWait           = 10 * time.Second
)

// ClientOption configures the SessionCast client.
type ClientOption func(*clientConfig)

type clientConfig struct {
	relayURL             string
	token                string
	machineID            string
	agentID              string
	label                string
	requiredCapabilities string
	pingInterval         time.Duration
	requestTimeout       time.Duration
	logger               *slog.Logger
}

// WithRelayURL sets the WebSocket relay URL.
func WithRelayURL(url string) ClientOption {
	return func(c *clientConfig) { c.relayURL = url }
}

// WithToken sets the agent authentication token.
func WithToken(token string) ClientOption {
	return func(c *clientConfig) { c.token = token }
}

// WithMachineID sets the machine identifier.
func WithMachineID(id string) ClientOption {
	return func(c *clientConfig) { c.machineID = id }
}

// WithAgentID sets the agent identifier for API mode.
func WithAgentID(id string) ClientOption {
	return func(c *clientConfig) { c.agentID = id }
}

// WithLabel sets an optional label for the connection.
func WithLabel(label string) ClientOption {
	return func(c *clientConfig) { c.label = label }
}

// WithRequiredCapabilities sets the capabilities to request (comma-separated).
func WithRequiredCapabilities(caps string) ClientOption {
	return func(c *clientConfig) { c.requiredCapabilities = caps }
}

// WithPingInterval sets how often to send ping messages.
func WithPingInterval(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.pingInterval = d }
}

// WithRequestTimeout sets the default timeout for API requests.
func WithRequestTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.requestTimeout = d }
}

// WithLogger sets a custom logger.
func WithLogger(logger *slog.Logger) ClientOption {
	return func(c *clientConfig) { c.logger = logger }
}

// Client is a SessionCast WebSocket client that connects to a relay server
// and communicates with a CLI agent.
type Client struct {
	cfg        clientConfig
	conn       *websocket.Conn
	corr       *correlator
	capNeg     *capabilityNegotiator
	writeMu    sync.Mutex
	connected  bool
	connMu     sync.RWMutex
	cancel     context.CancelFunc
	done       chan struct{}
	logger     *slog.Logger
	localMode  bool // true = no relay, direct tmux execution
}

// NewClient creates a new SessionCast client.
// If no token is provided, the client operates in local mode (no relay connection).
func NewClient(opts ...ClientOption) *Client {
	cfg := clientConfig{
		pingInterval:   defaultPingInterval,
		requestTimeout: defaultTimeout,
	}
	for _, o := range opts {
		o(&cfg)
	}

	logger := cfg.logger
	if logger == nil {
		logger = slog.Default()
	}

	localMode := cfg.token == ""

	return &Client{
		cfg:       cfg,
		corr:      newCorrelator(),
		capNeg:    newCapabilityNegotiator(),
		done:      make(chan struct{}),
		logger:    logger,
		localMode: localMode,
	}
}

// IsLocalMode returns true if the client is in local-direct mode (no relay).
func (c *Client) IsLocalMode() bool {
	return c.localMode
}

// Connect establishes a WebSocket connection and registers with the relay.
// In local mode (no token), this is a no-op.
func (c *Client) Connect(ctx context.Context) error {
	if c.localMode {
		c.logger.Info("local mode — no relay connection needed")
		c.setConnected(true)
		return nil
	}

	dialURL := c.cfg.relayURL

	// Append token as query parameter if present
	if c.cfg.token != "" {
		u, err := url.Parse(dialURL)
		if err == nil {
			q := u.Query()
			q.Set("token", c.cfg.token)
			u.RawQuery = q.Encode()
			dialURL = u.String()
		}
	}

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, dialURL, nil)
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}

	c.conn = conn
	c.setConnected(true)

	// Start receive loop
	innerCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	go c.readLoop(innerCtx)
	go c.pingLoop(innerCtx)

	// Send register message
	if err := c.register(); err != nil {
		c.Disconnect()
		return fmt.Errorf("register: %w", err)
	}

	return nil
}

// Disconnect closes the WebSocket connection.
func (c *Client) Disconnect() {
	c.connMu.Lock()
	wasConnected := c.connected
	c.connected = false
	c.connMu.Unlock()

	if wasConnected {
		if c.cancel != nil {
			c.cancel()
		}
		if c.conn != nil {
			c.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			c.conn.Close()
		}
	}
}

// IsConnected returns whether the client is connected.
func (c *Client) IsConnected() bool {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.connected
}

// WaitCapabilities blocks until capability negotiation completes.
func (c *Client) WaitCapabilities(timeout time.Duration) (*CapabilityResult, error) {
	return c.capNeg.Wait(timeout)
}

func (c *Client) setConnected(v bool) {
	c.connMu.Lock()
	c.connected = v
	c.connMu.Unlock()
}

func (c *Client) register() error {
	session := "api-" + c.cfg.agentID
	if c.cfg.machineID != "" && c.cfg.agentID == "" {
		session = c.cfg.machineID
	}

	msg := Message{
		Type:                 TypeRegister,
		Role:                 RoleHost,
		Session:              session,
		RequiredCapabilities: c.cfg.requiredCapabilities,
		Meta: &MessageMeta{
			Label:     c.cfg.label,
			MachineID: c.cfg.machineID,
			Token:     c.cfg.token,
		},
	}
	return c.sendMessage(msg)
}

func (c *Client) sendMessage(msg Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteJSON(msg)
}

func (c *Client) readLoop(ctx context.Context) {
	defer func() {
		c.setConnected(false)
		select {
		case <-c.done:
		default:
			close(c.done)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				c.logger.Error("websocket read error", "error", err)
			}
			return
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			c.logger.Warn("failed to unmarshal message", "error", err)
			continue
		}

		c.handleMessage(msg)
	}
}

func (c *Client) handleMessage(msg Message) {
	switch msg.Type {
	case TypePing:
		c.sendMessage(Message{Type: TypePong})

	case TypeAPIResponseStream:
		if msg.Meta != nil && msg.Meta.RequestID != "" {
			c.corr.SendStream(msg.Meta.RequestID, &ApiResponse{
				Payload: msg.Meta.Payload,
				Error:   msg.Meta.Error,
			})
		}

	case TypeAPIResponse:
		if msg.Meta != nil && msg.Meta.RequestID != "" {
			// If this requestId is registered for streaming, deliver the
			// final response through the stream channel and close it.
			if c.corr.IsStreaming(msg.Meta.RequestID) {
				c.corr.SendStream(msg.Meta.RequestID, &ApiResponse{
					Payload: msg.Meta.Payload,
					Error:   msg.Meta.Error,
				})
				c.corr.CompleteStream(msg.Meta.RequestID)
			} else {
				c.corr.Complete(msg.Meta.RequestID, &ApiResponse{
					Payload: msg.Meta.Payload,
					Error:   msg.Meta.Error,
				})
			}
		}

	case TypeCapabilityResult:
		if msg.Meta != nil {
			c.capNeg.handleResult(msg.Meta.Granted, msg.Meta.Denied)
		}

	case TypeError:
		c.logger.Error("server error", "meta", msg.Meta)
	}
}

func (c *Client) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.IsConnected() {
				c.sendMessage(Message{Type: TypePing})
			}
		}
	}
}
