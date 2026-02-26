package sessioncast

import (
	"fmt"
	"sync"
	"time"
)

// ApiResponse is the raw response received for a correlated request.
type ApiResponse struct {
	Payload string
	Error   string
}

// correlator manages request/response matching via requestId.
type correlator struct {
	mu       sync.Mutex
	pending  map[string]chan *ApiResponse
	timeouts map[string]*time.Timer
}

func newCorrelator() *correlator {
	return &correlator{
		pending:  make(map[string]chan *ApiResponse),
		timeouts: make(map[string]*time.Timer),
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
