# sessioncast-go

Go client library for the [SessionCast](https://sessioncast.io) relay server.

Connect to your local CLI agent via WebSocket and call LLM chat, run remote commands, send keystrokes to tmux sessions, and more — all from any Go program.

## Install

```bash
go get github.com/sessioncast/sessioncast-go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    sessioncast "github.com/sessioncast/sessioncast-go"
)

func main() {
    client := sessioncast.NewClient(
        sessioncast.WithRelayURL("wss://relay.sessioncast.io/ws"),
        sessioncast.WithToken("agt_YOUR_TOKEN"),
        sessioncast.WithMachineID("my-machine"),
    )

    ctx := context.Background()
    if err := client.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer client.Disconnect()

    resp, err := client.LlmChat(ctx, &sessioncast.LlmChatRequest{
        Model: "claude-code",
        Messages: []sessioncast.ChatMessage{
            {Role: "user", Content: "What is Docker? Answer in 2 sentences."},
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println(resp.Choices[0].Message.Content)
}
```

## Features

### LLM Chat

```go
resp, err := client.LlmChat(ctx, &sessioncast.LlmChatRequest{
    Model:       "claude-code",
    Messages:    []sessioncast.ChatMessage{{Role: "user", Content: "Hello"}},
    Temperature: 0.7,
    MaxTokens:   4096,
})
```

### LLM Streaming

```go
stream, err := client.LlmChatStream(ctx, &sessioncast.LlmChatRequest{
    Model:    "claude-code",
    Messages: []sessioncast.ChatMessage{
        {Role: "user", Content: "Write a hello world HTTP server in Go."},
    },
})
if err != nil {
    log.Fatal(err)
}

for event := range stream {
    if event.Error != nil {
        log.Fatal(event.Error)
    }
    if event.Chunk != nil {
        fmt.Print(event.Chunk.Content)
    }
    if event.Response != nil {
        fmt.Printf("\nTokens used: %d\n", event.Response.Usage.TotalTokens)
    }
}
```

### Remote Exec

```go
result, err := client.Exec(ctx, &sessioncast.ExecRequest{
    Command: "ls -la /tmp",
    Cwd:     "/home/user",
})
fmt.Printf("Exit: %d\nOutput:\n%s\n", result.ExitCode, result.Stdout)
```

### SendKeys

```go
result, err := client.SendKeys(ctx, &sessioncast.SendKeysRequest{
    Target: "my-session",
    Keys:   "echo hello",
    Enter:  true,
})
```

### List Sessions

```go
sessions, err := client.ListSessions(ctx)
for _, s := range sessions {
    fmt.Printf("%s (%d windows, attached=%v)\n", s.Name, s.Windows, s.Attached)
}
```

### Auto-Reconnect

For long-running services — exponential backoff (1s → 2s → 4s → ... → 30s max):

```go
client.ConnectWithReconnect(ctx, func() {
    fmt.Println("Connected!")
})
```

## Configuration

All options use the functional options pattern:

```go
sessioncast.NewClient(
    sessioncast.WithRelayURL("wss://relay.sessioncast.io/ws"),  // Relay URL
    sessioncast.WithToken("agt_YOUR_TOKEN"),                     // Agent token
    sessioncast.WithMachineID("my-machine"),                     // Machine ID
    sessioncast.WithAgentID("my-agent"),                         // Agent ID (API mode)
    sessioncast.WithLabel("My App"),                             // Connection label
    sessioncast.WithRequestTimeout(120 * time.Second),           // Request timeout (default: 30s)
    sessioncast.WithPingInterval(30 * time.Second),              // Keepalive interval (default: 30s)
    sessioncast.WithRequiredCapabilities("llm_chat,exec"),       // Capability negotiation
    sessioncast.WithLogger(slog.Default()),                      // Custom logger
)
```

## Architecture

```
Your Go App
  → sessioncast-go (this library)
    → WebSocket → SessionCast Relay
      → CLI Agent (target machine)
        → LLM / Shell / tmux
```

## Package Structure

| File | Purpose |
|------|---------|
| `client.go` | WebSocket client, connect/disconnect, message routing |
| `llm.go` | `LlmChat()` and `LlmChatStream()` |
| `exec.go` | `Exec()`, `SendKeys()`, `ListSessions()` |
| `protocol.go` | Message types, request/response structs |
| `correlator.go` | UUID-based request-response matching |
| `capability.go` | Capability negotiation handshake |
| `reconnect.go` | Auto-reconnect with exponential backoff |

## Dependencies

- [`gorilla/websocket`](https://github.com/gorilla/websocket) — WebSocket client
- [`google/uuid`](https://github.com/google/uuid) — Request ID generation

## Links

- [SessionCast](https://sessioncast.io) — Product page
- [SessionCast CLI](https://www.npmjs.com/package/sessioncast-cli) — The agent that receives requests
- [Developer Docs](https://developer.sessioncast.io/docs#go-sdk-quickstart) — Full API reference
- [OpenCode Integration](https://sessioncast.io/blog/2026-02-27-opencode-integration) — How this SDK powers the OpenCode provider

## License

MIT
