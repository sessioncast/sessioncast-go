package sessioncast

import (
	"context"
	"math"
	"time"
)

const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
	backoffFactor  = 2.0
)

// ConnectWithReconnect connects and automatically reconnects on disconnection.
// It blocks until the context is cancelled. The onConnect callback is called
// each time a connection is successfully established.
func (c *Client) ConnectWithReconnect(ctx context.Context, onConnect func()) error {
	attempt := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := c.Connect(ctx)
		if err != nil {
			c.logger.Warn("connection failed", "error", err, "attempt", attempt)
			attempt++
			c.waitBackoff(ctx, attempt)
			continue
		}

		// Connection successful
		attempt = 0
		if onConnect != nil {
			onConnect()
		}

		// Reset internals for reconnection
		c.corr = newCorrelator()
		c.capNeg = newCapabilityNegotiator()

		// Wait for disconnection
		select {
		case <-ctx.Done():
			c.Disconnect()
			return ctx.Err()
		case <-c.done:
			c.logger.Info("disconnected, attempting reconnect")
			c.done = make(chan struct{})
			attempt++
			c.waitBackoff(ctx, attempt)
		}
	}
}

func (c *Client) waitBackoff(ctx context.Context, attempt int) {
	backoff := time.Duration(float64(initialBackoff) * math.Pow(backoffFactor, float64(attempt-1)))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}

	c.logger.Info("reconnecting", "backoff", backoff, "attempt", attempt)

	select {
	case <-ctx.Done():
	case <-time.After(backoff):
	}
}
