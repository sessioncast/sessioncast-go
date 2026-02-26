package sessioncast

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// mockRelay simulates a SessionCast relay + CLI agent for testing.
type mockRelay struct {
	server     *httptest.Server
	upgrader   websocket.Upgrader
	mu         sync.Mutex
	clients    []*websocket.Conn
	registered bool
}

func newMockRelay(t *testing.T) *mockRelay {
	mr := &mockRelay{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}

	mr.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := mr.upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade error: %v", err)
			return
		}
		mr.mu.Lock()
		mr.clients = append(mr.clients, conn)
		mr.mu.Unlock()

		go mr.handleConn(t, conn)
	}))

	return mr
}

func (mr *mockRelay) URL() string {
	return "ws" + strings.TrimPrefix(mr.server.URL, "http")
}

func (mr *mockRelay) Close() {
	mr.server.Close()
}

func (mr *mockRelay) handleConn(t *testing.T, conn *websocket.Conn) {
	defer conn.Close()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Logf("mock relay: unmarshal error: %v", err)
			continue
		}

		t.Logf("mock relay received: type=%s", msg.Type)

		switch msg.Type {
		case TypeRegister:
			mr.mu.Lock()
			mr.registered = true
			mr.mu.Unlock()

			// Send capability_result
			if msg.RequiredCapabilities != "" {
				capResult := Message{
					Type: TypeCapabilityResult,
					Meta: &MessageMeta{
						Granted: msg.RequiredCapabilities,
						Denied:  "",
					},
				}
				conn.WriteJSON(capResult)
			}

		case TypePing:
			conn.WriteJSON(Message{Type: TypePong})

		case TypeExec:
			// Simulate exec response
			requestID := ""
			payload := ""
			if msg.Meta != nil {
				requestID = msg.Meta.RequestID
				payload = msg.Meta.Payload
			}

			var execReq ExecRequest
			json.Unmarshal([]byte(payload), &execReq)

			result := ExecResult{
				ExitCode: 0,
				Stdout:   fmt.Sprintf("mock output for: %s\n", execReq.Command),
				Stderr:   "",
				Duration: 42,
			}
			resultJSON, _ := json.Marshal(result)

			resp := Message{
				Type: TypeAPIResponse,
				Meta: &MessageMeta{
					RequestID: requestID,
					Payload:   string(resultJSON),
				},
			}
			conn.WriteJSON(resp)

		case TypeLLMChat:
			requestID := ""
			if msg.Meta != nil {
				requestID = msg.Meta.RequestID
			}

			chatResp := LlmChatResponse{
				ID:    "mock-chat-123",
				Model: "mock-model",
				Choices: []Choice{
					{
						Index:        0,
						Message:      ChatMessage{Role: "assistant", Content: "Hello from mock LLM!"},
						FinishReason: "stop",
					},
				},
				Usage: &Usage{
					PromptTokens:     10,
					CompletionTokens: 5,
					TotalTokens:      15,
				},
			}
			respJSON, _ := json.Marshal(chatResp)

			resp := Message{
				Type: TypeAPIResponse,
				Meta: &MessageMeta{
					RequestID: requestID,
					Payload:   string(respJSON),
				},
			}
			conn.WriteJSON(resp)

		case TypeSendKeys:
			requestID := ""
			if msg.Meta != nil {
				requestID = msg.Meta.RequestID
			}
			result := SendKeysResult{Success: true}
			resultJSON, _ := json.Marshal(result)
			resp := Message{
				Type: TypeAPIResponse,
				Meta: &MessageMeta{
					RequestID: requestID,
					Payload:   string(resultJSON),
				},
			}
			conn.WriteJSON(resp)

		case TypeListSessions:
			requestID := ""
			if msg.Meta != nil {
				requestID = msg.Meta.RequestID
			}
			sessions := []SessionInfo{
				{Name: "main", Windows: 2, Attached: true},
				{Name: "dev", Windows: 1, Attached: false},
			}
			sessionsJSON, _ := json.Marshal(sessions)
			resp := Message{
				Type: TypeAPIResponse,
				Meta: &MessageMeta{
					RequestID: requestID,
					Payload:   string(sessionsJSON),
				},
			}
			conn.WriteJSON(resp)
		}
	}
}

func TestMockRelayConnect(t *testing.T) {
	relay := newMockRelay(t)
	defer relay.Close()

	client := NewClient(
		WithRelayURL(relay.URL()),
		WithToken("test-token"),
		WithMachineID("test-machine"),
		WithAgentID("test-agent"),
		WithRequiredCapabilities("exec,llm_chat"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect()

	if !client.IsConnected() {
		t.Fatal("should be connected")
	}

	// Wait for capability negotiation
	caps, err := client.WaitCapabilities(3 * time.Second)
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}

	t.Logf("granted: %v, denied: %v", caps.Granted, caps.Denied)
	if len(caps.Granted) != 2 {
		t.Errorf("expected 2 granted capabilities, got %d", len(caps.Granted))
	}
}

func TestMockRelayExec(t *testing.T) {
	relay := newMockRelay(t)
	defer relay.Close()

	client := NewClient(
		WithRelayURL(relay.URL()),
		WithToken("test-token"),
		WithMachineID("test-machine"),
		WithAgentID("test-agent"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect()

	result, err := client.Exec(ctx, &ExecRequest{
		Command: "echo hello world",
		Cwd:     "/tmp",
	})
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	t.Logf("exec result: exitCode=%d, stdout=%q, duration=%d", result.ExitCode, result.Stdout, result.Duration)
	if result.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", result.ExitCode)
	}
	if !strings.Contains(result.Stdout, "echo hello world") {
		t.Errorf("stdout should contain command, got %q", result.Stdout)
	}
}

func TestMockRelayLlmChat(t *testing.T) {
	relay := newMockRelay(t)
	defer relay.Close()

	client := NewClient(
		WithRelayURL(relay.URL()),
		WithToken("test-token"),
		WithMachineID("test-machine"),
		WithAgentID("test-agent"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect()

	resp, err := client.LlmChat(ctx, &LlmChatRequest{
		Model: "test-model",
		Messages: []ChatMessage{
			{Role: "user", Content: "Say hello"},
		},
		MaxTokens: 100,
		Stream:    false,
	})
	if err != nil {
		t.Fatalf("llm_chat: %v", err)
	}

	t.Logf("llm response: id=%s, model=%s", resp.ID, resp.Model)
	if len(resp.Choices) == 0 {
		t.Fatal("no choices")
	}
	if resp.Choices[0].Message.Content != "Hello from mock LLM!" {
		t.Errorf("unexpected content: %q", resp.Choices[0].Message.Content)
	}
	if resp.Usage == nil || resp.Usage.TotalTokens != 15 {
		t.Errorf("unexpected usage: %+v", resp.Usage)
	}
}

func TestMockRelaySendKeys(t *testing.T) {
	relay := newMockRelay(t)
	defer relay.Close()

	client := NewClient(
		WithRelayURL(relay.URL()),
		WithToken("test-token"),
		WithMachineID("test-machine"),
		WithAgentID("test-agent"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect()

	result, err := client.SendKeys(ctx, &SendKeysRequest{
		Target: "main:0",
		Keys:   "ls -la",
		Enter:  true,
	})
	if err != nil {
		t.Fatalf("send_keys: %v", err)
	}
	if !result.Success {
		t.Error("expected success")
	}
}

func TestMockRelayListSessions(t *testing.T) {
	relay := newMockRelay(t)
	defer relay.Close()

	client := NewClient(
		WithRelayURL(relay.URL()),
		WithToken("test-token"),
		WithMachineID("test-machine"),
		WithAgentID("test-agent"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect()

	sessions, err := client.ListSessions(ctx)
	if err != nil {
		t.Fatalf("list_sessions: %v", err)
	}

	t.Logf("sessions: %+v", sessions)
	if len(sessions) != 2 {
		t.Errorf("expected 2 sessions, got %d", len(sessions))
	}
	if sessions[0].Name != "main" {
		t.Errorf("expected first session 'main', got %q", sessions[0].Name)
	}
}

func TestMockRelayMultipleConcurrentRequests(t *testing.T) {
	relay := newMockRelay(t)
	defer relay.Close()

	client := NewClient(
		WithRelayURL(relay.URL()),
		WithToken("test-token"),
		WithMachineID("test-machine"),
		WithAgentID("test-agent"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect()

	// Fire 5 concurrent exec requests
	var wg sync.WaitGroup
	errors := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			result, err := client.Exec(ctx, &ExecRequest{
				Command: fmt.Sprintf("echo request-%d", idx),
			})
			if err != nil {
				errors <- fmt.Errorf("exec %d: %v", idx, err)
				return
			}
			if result.ExitCode != 0 {
				errors <- fmt.Errorf("exec %d: exit code %d", idx, result.ExitCode)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}
