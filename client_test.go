package sessioncast

import (
	"encoding/json"
	"testing"
	"time"
)

func TestProtocolMarshal(t *testing.T) {
	msg := Message{
		Type:    TypeRegister,
		Role:    RoleHost,
		Session: "api-test-agent",
		RequiredCapabilities: "exec,llm_chat",
		Meta: &MessageMeta{
			Label:     "test",
			MachineID: "test-machine",
			Token:     "agt_test",
		},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal register message: %v", err)
	}

	var decoded Message
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal register message: %v", err)
	}

	if decoded.Type != TypeRegister {
		t.Errorf("expected type %q, got %q", TypeRegister, decoded.Type)
	}
	if decoded.Role != RoleHost {
		t.Errorf("expected role %q, got %q", RoleHost, decoded.Role)
	}
	if decoded.Meta.Token != "agt_test" {
		t.Errorf("expected token %q, got %q", "agt_test", decoded.Meta.Token)
	}
}

func TestLlmChatRequestPayload(t *testing.T) {
	req := &LlmChatRequest{
		Model: "gpt-4",
		Messages: []ChatMessage{
			{Role: "system", Content: "You are helpful"},
			{Role: "user", Content: "Hello"},
		},
		Temperature: 0.7,
		MaxTokens:   2048,
		Stream:      false,
	}

	payload, err := marshalPayload(req)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var decoded LlmChatRequest
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if decoded.Model != "gpt-4" {
		t.Errorf("expected model %q, got %q", "gpt-4", decoded.Model)
	}
	if len(decoded.Messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(decoded.Messages))
	}
}

func TestExecRequestPayload(t *testing.T) {
	req := &ExecRequest{
		Command: "ls -la /tmp",
		Cwd:     "/home/user",
		Timeout: 30000,
	}

	payload, err := marshalPayload(req)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var decoded ExecRequest
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if decoded.Command != "ls -la /tmp" {
		t.Errorf("expected command %q, got %q", "ls -la /tmp", decoded.Command)
	}
}

func TestCorrelatorBasic(t *testing.T) {
	c := newCorrelator()

	ch := c.Register("req-1", 5*time.Second)
	go func() {
		time.Sleep(10 * time.Millisecond)
		c.Complete("req-1", &ApiResponse{
			Payload: `{"exitCode":0}`,
		})
	}()

	resp := <-ch
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if resp.Payload != `{"exitCode":0}` {
		t.Errorf("unexpected payload: %s", resp.Payload)
	}
}

func TestCorrelatorTimeout(t *testing.T) {
	c := newCorrelator()

	ch := c.Register("req-timeout", 50*time.Millisecond)

	resp := <-ch
	if resp.Error == "" {
		t.Fatal("expected timeout error")
	}
}

func TestCorrelatorCancel(t *testing.T) {
	c := newCorrelator()

	_ = c.Register("req-cancel", 5*time.Second)
	c.Cancel("req-cancel")

	if c.PendingCount() != 0 {
		t.Errorf("expected 0 pending, got %d", c.PendingCount())
	}
}

func TestCapabilityNegotiator(t *testing.T) {
	cn := newCapabilityNegotiator()

	go func() {
		time.Sleep(10 * time.Millisecond)
		cn.handleResult("exec,llm_chat", "send_keys")
	}()

	result, err := cn.Wait(5 * time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Granted) != 2 {
		t.Errorf("expected 2 granted, got %d", len(result.Granted))
	}
	if len(result.Denied) != 1 {
		t.Errorf("expected 1 denied, got %d", len(result.Denied))
	}

	if !cn.HasCapability("exec") {
		t.Error("expected exec capability")
	}
	if cn.HasCapability("send_keys") {
		t.Error("send_keys should be denied")
	}
}

func TestSplitCapabilities(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", nil},
		{"exec", []string{"exec"}},
		{"exec,llm_chat,send_keys", []string{"exec", "llm_chat", "send_keys"}},
		{" exec , llm_chat ", []string{"exec", "llm_chat"}},
	}

	for _, tt := range tests {
		result := splitCapabilities(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("splitCapabilities(%q): expected %d items, got %d", tt.input, len(tt.expected), len(result))
			continue
		}
		for i, v := range result {
			if v != tt.expected[i] {
				t.Errorf("splitCapabilities(%q)[%d]: expected %q, got %q", tt.input, i, tt.expected[i], v)
			}
		}
	}
}

func TestLlmChatResponseParsing(t *testing.T) {
	raw := `{
		"id": "chatcmpl-123",
		"object": "chat.completion",
		"model": "gpt-4",
		"choices": [{
			"index": 0,
			"message": {"role": "assistant", "content": "Hello world"},
			"finish_reason": "stop"
		}],
		"usage": {
			"prompt_tokens": 10,
			"completion_tokens": 5,
			"total_tokens": 15
		}
	}`

	var resp LlmChatResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp.ID != "chatcmpl-123" {
		t.Errorf("expected id %q, got %q", "chatcmpl-123", resp.ID)
	}
	if len(resp.Choices) != 1 {
		t.Fatalf("expected 1 choice, got %d", len(resp.Choices))
	}
	if resp.Choices[0].Message.Content != "Hello world" {
		t.Errorf("expected content %q, got %q", "Hello world", resp.Choices[0].Message.Content)
	}
	if resp.Usage.TotalTokens != 15 {
		t.Errorf("expected total_tokens 15, got %d", resp.Usage.TotalTokens)
	}
}
