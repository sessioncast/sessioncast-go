package sessioncast

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// Exec runs a shell command on the remote CLI agent and returns the result.
func (c *Client) Exec(ctx context.Context, req *ExecRequest) (*ExecResult, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}

	requestID := uuid.New().String()
	payload, err := marshalPayload(req)
	if err != nil {
		return nil, fmt.Errorf("marshal exec payload: %w", err)
	}

	respCh := c.corr.Register(requestID, c.cfg.requestTimeout)

	msg := Message{
		Type: TypeExec,
		Meta: &MessageMeta{
			RequestID: requestID,
			Payload:   payload,
		},
	}
	if err := c.sendMessage(msg); err != nil {
		c.corr.Cancel(requestID)
		return nil, fmt.Errorf("send exec: %w", err)
	}

	select {
	case <-ctx.Done():
		c.corr.Cancel(requestID)
		return nil, ctx.Err()
	case resp := <-respCh:
		if resp.Error != "" {
			return nil, fmt.Errorf("exec error: %s", resp.Error)
		}

		var result ExecResult
		if err := json.Unmarshal([]byte(resp.Payload), &result); err != nil {
			return nil, fmt.Errorf("unmarshal exec result: %w", err)
		}
		return &result, nil
	}
}

// SendKeys sends keystrokes to a tmux session via the CLI agent.
func (c *Client) SendKeys(ctx context.Context, req *SendKeysRequest) (*SendKeysResult, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}

	requestID := uuid.New().String()
	payload, err := marshalPayload(req)
	if err != nil {
		return nil, fmt.Errorf("marshal send_keys payload: %w", err)
	}

	respCh := c.corr.Register(requestID, c.cfg.requestTimeout)

	msg := Message{
		Type: TypeSendKeys,
		Meta: &MessageMeta{
			RequestID: requestID,
			Payload:   payload,
		},
	}
	if err := c.sendMessage(msg); err != nil {
		c.corr.Cancel(requestID)
		return nil, fmt.Errorf("send send_keys: %w", err)
	}

	select {
	case <-ctx.Done():
		c.corr.Cancel(requestID)
		return nil, ctx.Err()
	case resp := <-respCh:
		if resp.Error != "" {
			return nil, fmt.Errorf("send_keys error: %s", resp.Error)
		}

		var result SendKeysResult
		if err := json.Unmarshal([]byte(resp.Payload), &result); err != nil {
			return nil, fmt.Errorf("unmarshal send_keys result: %w", err)
		}
		return &result, nil
	}
}

// ListSessions retrieves the list of active tmux sessions from the CLI agent.
func (c *Client) ListSessions(ctx context.Context) ([]SessionInfo, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}

	requestID := uuid.New().String()
	payload, err := marshalPayload(struct{}{})
	if err != nil {
		return nil, fmt.Errorf("marshal list_sessions payload: %w", err)
	}

	respCh := c.corr.Register(requestID, c.cfg.requestTimeout)

	msg := Message{
		Type: TypeListSessions,
		Meta: &MessageMeta{
			RequestID: requestID,
			Payload:   payload,
		},
	}
	if err := c.sendMessage(msg); err != nil {
		c.corr.Cancel(requestID)
		return nil, fmt.Errorf("send list_sessions: %w", err)
	}

	select {
	case <-ctx.Done():
		c.corr.Cancel(requestID)
		return nil, ctx.Err()
	case resp := <-respCh:
		if resp.Error != "" {
			return nil, fmt.Errorf("list_sessions error: %s", resp.Error)
		}

		var sessions []SessionInfo
		if err := json.Unmarshal([]byte(resp.Payload), &sessions); err != nil {
			return nil, fmt.Errorf("unmarshal list_sessions result: %w", err)
		}
		return sessions, nil
	}
}
