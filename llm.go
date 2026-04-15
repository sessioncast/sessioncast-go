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

// LlmStreamEvent represents an event in the streaming LLM response.
type LlmStreamEvent struct {
	// Chunk is set for api_response_stream messages (partial content).
	Chunk *LlmStreamChunk

	// Response is set for the final api_response message (complete response).
	Response *LlmChatResponse

	// Error is set when a protocol or parsing error occurs.
	Error error
}

// LlmChatStream sends a streaming chat completion request and returns a channel
// that receives incremental chunks followed by the final complete response.
// The request payload includes stream: true.
// The channel is closed when the final response arrives or an error occurs.
func (c *Client) LlmChatStream(ctx context.Context, req *LlmChatRequest) (<-chan LlmStreamEvent, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}

	// Force streaming flag on the request
	req.Stream = true

	requestID := uuid.New().String()
	payload, err := marshalPayload(req)
	if err != nil {
		return nil, fmt.Errorf("marshal llm_chat payload: %w", err)
	}

	// Register for streaming before sending
	rawCh := c.corr.RegisterStream(requestID, c.cfg.requestTimeout)

	// Send the llm_chat message
	msg := Message{
		Type: TypeLLMChat,
		Meta: &MessageMeta{
			RequestID: requestID,
			Payload:   payload,
		},
	}
	if err := c.sendMessage(msg); err != nil {
		c.corr.CancelStream(requestID)
		return nil, fmt.Errorf("send llm_chat stream: %w", err)
	}

	eventCh := make(chan LlmStreamEvent, 64)

	go func() {
		defer close(eventCh)

		for {
			select {
			case <-ctx.Done():
				c.corr.CancelStream(requestID)
				eventCh <- LlmStreamEvent{Error: ctx.Err()}
				return

			case raw, ok := <-rawCh:
				if !ok {
					// Channel closed — stream finished
					return
				}

				if raw.Error != "" {
					eventCh <- LlmStreamEvent{Error: fmt.Errorf("llm_chat stream error: %s", raw.Error)}
					return
				}

				// Try to parse as a stream chunk first
				var chunk LlmStreamChunk
				if err := json.Unmarshal([]byte(raw.Payload), &chunk); err == nil && (chunk.Content != "" || chunk.Done) {
					if chunk.Error != "" {
						eventCh <- LlmStreamEvent{Error: fmt.Errorf("llm_chat stream chunk error: %s", chunk.Error)}
						return
					}
					eventCh <- LlmStreamEvent{Chunk: &chunk}
					continue
				}

				// If not a chunk, try to parse as a complete LlmChatResponse
				var chatResp LlmChatResponse
				if err := json.Unmarshal([]byte(raw.Payload), &chatResp); err != nil {
					eventCh <- LlmStreamEvent{Error: fmt.Errorf("unmarshal stream payload: %w", err)}
					return
				}

				if chatResp.Error != nil {
					eventCh <- LlmStreamEvent{Error: chatResp.Error}
					return
				}

				eventCh <- LlmStreamEvent{Response: &chatResp}
				// Final response has been received; the rawCh will be closed
				// by CompleteStream in handleMessage.
			}
		}
	}()

	return eventCh, nil
}
