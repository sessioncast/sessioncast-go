package sessioncast

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// capabilityNegotiator manages the capability handshake with the CLI agent.
type capabilityNegotiator struct {
	mu      sync.Mutex
	result  *CapabilityResult
	waitCh  chan struct{}
	done    bool
}

func newCapabilityNegotiator() *capabilityNegotiator {
	return &capabilityNegotiator{
		waitCh: make(chan struct{}),
	}
}

// handleResult processes a capability_result message from the relay.
func (cn *capabilityNegotiator) handleResult(granted, denied string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	if cn.done {
		return
	}

	cn.result = &CapabilityResult{
		Granted: splitCapabilities(granted),
		Denied:  splitCapabilities(denied),
	}
	cn.done = true
	close(cn.waitCh)
}

// Wait blocks until capability negotiation completes or timeout.
func (cn *capabilityNegotiator) Wait(timeout time.Duration) (*CapabilityResult, error) {
	select {
	case <-cn.waitCh:
		cn.mu.Lock()
		defer cn.mu.Unlock()
		return cn.result, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("capability negotiation timed out after %s", timeout)
	}
}

// HasCapability checks if a specific capability was granted.
func (cn *capabilityNegotiator) HasCapability(cap string) bool {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	if cn.result == nil {
		return false
	}
	for _, g := range cn.result.Granted {
		if g == cap {
			return true
		}
	}
	return false
}

func splitCapabilities(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
