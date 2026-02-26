package sessioncast

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// LlmChat sends a chat completion request through the SessionCast CLI agent
// and returns the response synchronously.
func (c *Client) LlmChat(ctx context.Context, req *LlmChatRequest) (*LlmChatResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}

	requestID := uuid.New().String()
	payload, err := marshalPayload(req)
	if err != nil {
		return nil, fmt.Errorf("marshal llm_chat payload: %w", err)
	}

	// Register for response before sending
	respCh := c.corr.Register(requestID, c.cfg.requestTimeout)

	// Send the llm_chat message
	msg := Message{
		Type: TypeLLMChat,
		Meta: &MessageMeta{
			RequestID: requestID,
			Payload:   payload,
		},
	}
	if err := c.sendMessage(msg); err != nil {
		c.corr.Cancel(requestID)
		return nil, fmt.Errorf("send llm_chat: %w", err)
	}

	// Wait for response
	select {
	case <-ctx.Done():
		c.corr.Cancel(requestID)
		return nil, ctx.Err()
	case resp := <-respCh:
		if resp.Error != "" {
			return nil, fmt.Errorf("llm_chat error: %s", resp.Error)
		}

		var chatResp LlmChatResponse
		if err := json.Unmarshal([]byte(resp.Payload), &chatResp); err != nil {
			return nil, fmt.Errorf("unmarshal llm_chat response: %w", err)
		}

		if chatResp.Error != nil {
			return nil, chatResp.Error
		}

		return &chatResp, nil
	}
}
