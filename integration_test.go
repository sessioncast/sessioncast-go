//go:build integration

package sessioncast

import (
	"context"
	"os"
	"testing"
	"time"
)

// Run with: go test -tags integration -v -run TestIntegration
// Requires env vars: SESSIONCAST_RELAY_URL, SESSIONCAST_TOKEN

func getTestClient(t *testing.T) *Client {
	relayURL := os.Getenv("SESSIONCAST_RELAY_URL")
	token := os.Getenv("SESSIONCAST_TOKEN")
	if relayURL == "" || token == "" {
		t.Skip("SESSIONCAST_RELAY_URL and SESSIONCAST_TOKEN required")
	}

	return NewClient(
		WithRelayURL(relayURL),
		WithToken(token),
		WithMachineID("integration-test"),
		WithAgentID("integration-test"),
		WithRequiredCapabilities("exec,llm_chat"),
		WithRequestTimeout(60*time.Second),
	)
}

func TestIntegrationConnect(t *testing.T) {
	client := getTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Disconnect()

	if !client.IsConnected() {
		t.Fatal("client should be connected")
	}
	t.Log("Connected to relay successfully")
}

func TestIntegrationExec(t *testing.T) {
	client := getTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Disconnect()

	// Wait for capability negotiation
	caps, err := client.WaitCapabilities(10 * time.Second)
	if err != nil {
		t.Logf("capability wait: %v (continuing anyway)", err)
	} else {
		t.Logf("capabilities granted: %v, denied: %v", caps.Granted, caps.Denied)
	}

	result, err := client.Exec(ctx, &ExecRequest{
		Command: "echo hello-from-sessioncast-go",
	})
	if err != nil {
		t.Fatalf("exec failed: %v", err)
	}

	t.Logf("exec result: exitCode=%d, stdout=%q, duration=%dms", result.ExitCode, result.Stdout, result.Duration)
	if result.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", result.ExitCode)
	}
}

func TestIntegrationLlmChat(t *testing.T) {
	client := getTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Disconnect()

	// Wait for capability negotiation
	caps, err := client.WaitCapabilities(10 * time.Second)
	if err != nil {
		t.Logf("capability wait: %v (continuing anyway)", err)
	} else {
		t.Logf("capabilities granted: %v, denied: %v", caps.Granted, caps.Denied)
	}

	// Use the model from env or fallback to llama3.2
	model := os.Getenv("SESSIONCAST_LLM_MODEL")
	if model == "" {
		model = "llama3.2"
	}
	resp, err := client.LlmChat(ctx, &LlmChatRequest{
		Model: model,
		Messages: []ChatMessage{
			{Role: "user", Content: "Say exactly: hello-sessioncast-go-test"},
		},
		MaxTokens: 50,
		Stream:    false,
	})
	if err != nil {
		t.Fatalf("llm_chat failed: %v", err)
	}

	t.Logf("llm_chat response: id=%s, model=%s", resp.ID, resp.Model)
	if len(resp.Choices) > 0 {
		t.Logf("content: %s", resp.Choices[0].Message.Content)
	} else {
		t.Error("no choices in response")
	}
	if resp.Usage != nil {
		t.Logf("usage: prompt=%d, completion=%d, total=%d",
			resp.Usage.PromptTokens, resp.Usage.CompletionTokens, resp.Usage.TotalTokens)
	}
}
