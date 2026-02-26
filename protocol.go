package sessioncast

import "encoding/json"

// Message types
const (
	TypeRegister          = "register"
	TypePing              = "ping"
	TypePong              = "pong"
	TypeExec              = "exec"
	TypeLLMChat           = "llm_chat"
	TypeSendKeys          = "send_keys"
	TypeListSessions      = "list_sessions"
	TypeAPIResponse       = "api_response"
	TypeCapabilityRequest = "capability_request"
	TypeCapabilityGrant   = "capability_grant"
	TypeCapabilityResult  = "capability_result"
	TypeError             = "error"
)

// Roles
const (
	RoleHost   = "host"
	RoleViewer = "viewer"
)

// Message is the top-level WebSocket message envelope.
type Message struct {
	Type                 string       `json:"type"`
	Role                 string       `json:"role,omitempty"`
	Session              string       `json:"session,omitempty"`
	Payload              string       `json:"payload,omitempty"`
	Meta                 *MessageMeta `json:"meta,omitempty"`
	RequiredCapabilities string       `json:"requiredCapabilities,omitempty"`
}

// MessageMeta carries per-message metadata.
type MessageMeta struct {
	RequestID    string `json:"requestId,omitempty"`
	Payload      string `json:"payload,omitempty"`
	Error        string `json:"error,omitempty"`
	From         string `json:"from,omitempty"`
	Capabilities string `json:"capabilities,omitempty"`
	Granted      string `json:"granted,omitempty"`
	Denied       string `json:"denied,omitempty"`
	Label        string `json:"label,omitempty"`
	MachineID    string `json:"machineId,omitempty"`
	Token        string `json:"token,omitempty"`
}

// --- LLM Chat ---

// LlmChatRequest is the payload for an llm_chat message.
type LlmChatRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Temperature float64       `json:"temperature,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	Stream      bool          `json:"stream"`
}

// ChatMessage represents a single message in a chat conversation.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// LlmChatResponse is the parsed payload from an api_response to an llm_chat request.
type LlmChatResponse struct {
	ID      string    `json:"id"`
	Object  string    `json:"object"`
	Created int64     `json:"created"`
	Model   string    `json:"model"`
	Choices []Choice  `json:"choices"`
	Usage   *Usage    `json:"usage,omitempty"`
	Error   *LlmError `json:"error,omitempty"`
}

// Choice represents a single completion choice.
type Choice struct {
	Index        int          `json:"index"`
	Message      ChatMessage  `json:"message"`
	FinishReason string       `json:"finish_reason"`
}

// Usage holds token usage information.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// LlmError represents an error from the LLM provider.
type LlmError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

func (e *LlmError) Error() string {
	return e.Message
}

// --- Exec ---

// ExecRequest is the payload for an exec message.
type ExecRequest struct {
	Command   string `json:"command"`
	Cwd       string `json:"cwd,omitempty"`
	Timeout   int64  `json:"timeout,omitempty"`
	SessionID string `json:"sessionId,omitempty"`
}

// ExecResult is the parsed payload from an api_response to an exec request.
type ExecResult struct {
	ExitCode int    `json:"exitCode"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	Duration int64  `json:"duration"`
}

// --- SendKeys ---

// SendKeysRequest is the payload for a send_keys message.
type SendKeysRequest struct {
	Target string `json:"target,omitempty"`
	Keys   string `json:"keys"`
	Enter  bool   `json:"enter"`
}

// SendKeysResult is the parsed payload from an api_response to a send_keys request.
type SendKeysResult struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// --- List Sessions ---

// SessionInfo describes a tmux session.
type SessionInfo struct {
	Name     string `json:"name"`
	Windows  int    `json:"windows"`
	Attached bool   `json:"attached"`
}

// --- Capability ---

// CapabilityResult represents the result of capability negotiation.
type CapabilityResult struct {
	Granted []string
	Denied  []string
}

// marshalPayload serializes a value to a JSON string for use in meta.payload.
func marshalPayload(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
