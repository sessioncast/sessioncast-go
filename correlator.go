package sessioncast

import (
	"fmt"
	"sync"
	"time"
)

// streamEntry tracks a streaming request's channel and timeout.
type streamEntry struct {
	ch    chan *ApiResponse
	timer *time.Timer
}

// ApiResponse is the raw response received for a correlated request.
type ApiResponse struct {
	Payload string
	Error   string
}

// correlator manages request/response matching via requestId.
type correlator struct {
	mu        sync.Mutex
	pending   map[string]chan *ApiResponse
	timeouts  map[string]*time.Timer
	streaming map[string]*streamEntry
}

func newCorrelator() *correlator {
	return &correlator{
		pending:   make(map[string]chan *ApiResponse),
		timeouts:  make(map[string]*time.Timer),
		streaming: make(map[string]*streamEntry),
	}
}

// Register creates a response channel for the given requestId.
// It returns a channel that will receive exactly one ApiResponse.
// If no response arrives within timeout, the channel receives an error response.
func (c *correlator) Register(requestID string, timeout time.Duration) <-chan *ApiResponse {
	ch := make(chan *ApiResponse, 1)

	c.mu.Lock()
	c.pending[requestID] = ch
	timer := time.AfterFunc(timeout, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if pending, ok := c.pending[requestID]; ok {
			pending <- &ApiResponse{
				Error: fmt.Sprintf("request %s timed out after %s", requestID, timeout),
			}
			delete(c.pending, requestID)
			delete(c.timeouts, requestID)
		}
	})
	c.timeouts[requestID] = timer
	c.mu.Unlock()

	return ch
}

// Complete delivers a response to the waiting channel for requestId.
func (c *correlator) Complete(requestID string, resp *ApiResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ch, ok := c.pending[requestID]; ok {
		if timer, exists := c.timeouts[requestID]; exists {
			timer.Stop()
			delete(c.timeouts, requestID)
		}
		ch <- resp
		delete(c.pending, requestID)
	}
}

// Cancel removes a pending request without completing it.
func (c *correlator) Cancel(requestID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if timer, exists := c.timeouts[requestID]; exists {
		timer.Stop()
		delete(c.timeouts, requestID)
	}
	delete(c.pending, requestID)
}

// PendingCount returns the number of in-flight requests.
func (c *correlator) PendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// RegisterStream creates a channel that can receive multiple ApiResponse messages
// for streaming. The channel is closed when CancelStream or CompleteStream is called,
// or when the timeout expires.
func (c *correlator) RegisterStream(requestID string, timeout time.Duration) <-chan *ApiResponse {
	ch := make(chan *ApiResponse, 64)

	c.mu.Lock()
	timer := time.AfterFunc(timeout, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if entry, ok := c.streaming[requestID]; ok {
			entry.ch <- &ApiResponse{
				Error: fmt.Sprintf("stream request %s timed out after %s", requestID, timeout),
			}
			close(entry.ch)
			delete(c.streaming, requestID)
		}
	})
	c.streaming[requestID] = &streamEntry{ch: ch, timer: timer}
	c.mu.Unlock()

	return ch
}

// SendStream delivers a streaming chunk to the channel for requestId.
// Returns false if the requestId is not registered for streaming.
func (c *correlator) SendStream(requestID string, resp *ApiResponse) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.streaming[requestID]
	if !ok {
		return false
	}
	entry.ch <- resp
	return true
}

// CompleteStream closes the streaming channel for requestId, signaling no more data.
func (c *correlator) CompleteStream(requestID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.streaming[requestID]; ok {
		entry.timer.Stop()
		close(entry.ch)
		delete(c.streaming, requestID)
	}
}

// CancelStream removes a streaming request and closes its channel.
func (c *correlator) CancelStream(requestID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.streaming[requestID]; ok {
		entry.timer.Stop()
		close(entry.ch)
		delete(c.streaming, requestID)
	}
}

// IsStreaming checks if a requestId is registered for streaming.
func (c *correlator) IsStreaming(requestID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.streaming[requestID]
	return ok
}
