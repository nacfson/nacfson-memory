package embedding

import (
	"log/slog"
	"time"
)

// Outcomes of one embed_call event.
const (
	EmbedOutcomeOK          = "ok"
	EmbedOutcomeBreakerOpen = "breaker_open"
	EmbedOutcomeError       = "error"
)

// EmbedCall is the metadata of one embedding request. InputChars counts the characters sent
// to the embedder (0 when the breaker short-circuits the call), never the text itself, so
// query and memory text cannot reach the telemetry log.
type EmbedCall struct {
	Caller      string
	WorkspaceID string
	SpaceID     string
	InputChars  int
	Outcome     string
	Duration    time.Duration
}

// Log emits c as a metadata-only embed_call event. A nil logger is silent.
func (c EmbedCall) Log(logger *slog.Logger) {
	if logger == nil {
		return
	}
	logger.Info("embed_call",
		"caller", c.Caller,
		"workspace_id", c.WorkspaceID,
		"space_id", c.SpaceID,
		"input_chars", c.InputChars,
		"outcome", c.Outcome,
		"duration_ms", c.Duration.Milliseconds(),
	)
}
