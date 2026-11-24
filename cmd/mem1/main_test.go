package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nacfson/nacfson-memory/internal/service"
	"github.com/nacfson/nacfson-memory/internal/storage"
)

func TestCLIUsageAndValidation(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := run(nil, &out, &errBuf); code != 2 {
		t.Fatalf("expected exit 2 on empty args, got %d", code)
	}
	out.Reset()
	errBuf.Reset()
	if code := run([]string{"help"}, &out, &errBuf); code != 0 || !strings.Contains(out.String(), "serve") || !strings.Contains(out.String(), "mcp-stdio") {
		t.Fatalf("expected help output with phase 2 commands, code=%d out=%s", code, out.String())
	}
	if code := run([]string{"unknown-cmd"}, &out, &errBuf); code != 2 {
		t.Fatalf("expected exit 2 on unknown command, got %d", code)
	}

	memID := uuid.New().String()
	badCalls := [][]string{
		{"explain"},
		{"explain", "--id=bad"},
		{"revert", "--id=" + memID, "--to-version=1"},
		{"purge", "--id=" + memID},
		{"confirm-token", "--workspace=app"},
		{"confirm-token", "--workspace=app", "--action=purge", "--target=not-a-uuid"},
		{"dead-letters", "--limit=0"},
		{"list", "--limit=0"},
		{"eval", "--max-tokens=9999"},
		{"reindex", "--batch-size=0"},
	}
	for _, args := range badCalls {
		out.Reset()
		errBuf.Reset()
		if code := run(args, &out, &errBuf); code != 2 {
			t.Fatalf("args %v expected exit 2, got %d (stderr=%s)", args, code, errBuf.String())
		}
	}

	// Missing MEM1_ADMIN_TOKEN fails admin commands cleanly before HTTP dial.
	t.Setenv("MEM1_ADMIN_TOKEN", "")
	out.Reset()
	errBuf.Reset()
	if code := run([]string{"dead-letters"}, &out, &errBuf); code != 1 || !strings.Contains(errBuf.String(), "MEM1_ADMIN_TOKEN") {
		t.Fatalf("expected MEM1_ADMIN_TOKEN error, code=%d stderr=%s", code, errBuf.String())
	}

	if got := adminBaseURL(":8787"); got != "http://127.0.0.1:8787" {
		t.Fatalf("adminBaseURL(:8787) = %q, want http://127.0.0.1:8787", got)
	}
	if got := adminBaseURL("127.0.0.1:9000/"); got != "http://127.0.0.1:9000" {
		t.Fatalf("adminBaseURL(127.0.0.1:9000/) = %q, want http://127.0.0.1:9000", got)
	}
}

func TestOperatorCLICommandsAgainstHTTPServer(t *testing.T) {
	memID := uuid.MustParse("019a0000-0000-7000-8000-000000000111")
	targetID := uuid.MustParse("019a0000-0000-7000-8000-000000000222")
	const adminToken = "admin-secret-token"

	var lastMethod, lastPath, lastAuth string
	var lastPayload map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastMethod = r.Method
		lastPath = r.URL.RequestURI()
		lastAuth = r.Header.Get("Authorization")
		lastPayload = nil
		if r.Body != nil && r.ContentLength != 0 {
			_ = json.NewDecoder(r.Body).Decode(&lastPayload)
		}
		if lastAuth != "Bearer "+adminToken {
			http.Error(w, `{"error":"unauthorized","code":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/memories":
			_ = json.NewEncoder(w).Encode(storage.ListMemoriesPage{
				Items: []storage.MemoryListItem{{
					Memory: storage.Memory{
						MemoryID:       memID,
						WorkspaceID:    "payments",
						Durability:     storage.DurabilityDurable,
						TopicKey:       "primary-db",
						Status:         storage.StatusActive,
						CurrentVersion: 2,
						SummaryStub:    "PostgreSQL 17 primary DB",
						PremiseDrifted: true,
					},
				}},
				NextCursor: "cur-1",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/memories/"+memID.String()+"/explain":
			_ = json.NewEncoder(w).Encode(map[string]any{"memory_id": memID, "current_version": 2})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/memories/"+memID.String()+"/revert":
			_ = json.NewEncoder(w).Encode(map[string]any{"memory_id": memID, "version": 3, "restored_from_version": 1})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/memories/"+memID.String()+"/purge":
			_ = json.NewEncoder(w).Encode(map[string]any{"purged": true, "memory_id": memID, "deleted_inbox_count": 2})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/auth/confirm-token":
			_ = json.NewEncoder(w).Encode(service.ConfirmTokenResult{
				ConfirmationToken: "tok-abc",
				ExpiresAt:         time.Date(2026, 11, 24, 15, 0, 0, 0, time.UTC),
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/ops/dead-letters":
			_ = json.NewEncoder(w).Encode(storage.DeadLettersReport{
				DeadLetters:   []storage.DeadLetterInboxItem{},
				DiscardedJobs: []storage.DiscardedRiverJob{},
			})
		default:
			http.Error(w, `{"error":"not found","code":"not_found"}`, http.StatusNotFound)
		}
	}))
	defer srv.Close()

	t.Setenv("MEM1_HTTP_ADDR", srv.URL)
	t.Setenv("MEM1_ADMIN_TOKEN", adminToken)

	var out, errBuf bytes.Buffer
	if code := run([]string{"list", "--workspace=payments", "--durability=durable", "--limit=25"}, &out, &errBuf); code != 0 {
		t.Fatalf("list text failed: code=%d stderr=%s", code, errBuf.String())
	}
	if !strings.Contains(out.String(), "primary-db") || !strings.Contains(out.String(), "[DRIFTED]") || !strings.Contains(lastPath, "workspace_id=payments") {
		t.Fatalf("unexpected list output=%q path=%q", out.String(), lastPath)
	}

	out.Reset()
	errBuf.Reset()
	if code := run([]string{"list", "--json", "--include-revisions", "--premise-drifted=true"}, &out, &errBuf); code != 0 || !strings.Contains(out.String(), `"next_cursor": "cur-1"`) {
		t.Fatalf("list --json failed: code=%d out=%s stderr=%s", code, out.String(), errBuf.String())
	}

	out.Reset()
	errBuf.Reset()
	if code := run([]string{"explain", "--id=" + memID.String()}, &out, &errBuf); code != 0 || lastMethod != http.MethodGet {
		t.Fatalf("explain failed: code=%d stderr=%s", code, errBuf.String())
	}

	out.Reset()
	errBuf.Reset()
	if code := run([]string{"revert", "--id=" + memID.String(), "--to-version=1", "--expected-version=2", "--reason=fix"}, &out, &errBuf); code != 0 {
		t.Fatalf("revert failed: code=%d stderr=%s", code, errBuf.String())
	}
	if lastPayload["target_version"] != float64(1) || lastPayload["expected_version"] != float64(2) {
		t.Fatalf("unexpected revert payload: %+v", lastPayload)
	}

	out.Reset()
	errBuf.Reset()
	if code := run([]string{"purge", "--id=" + memID.String(), "--yes", "--reason=gdpr"}, &out, &errBuf); code != 0 || lastPayload["confirm"] != true {
		t.Fatalf("purge failed: code=%d payload=%+v stderr=%s", code, lastPayload, errBuf.String())
	}

	out.Reset()
	errBuf.Reset()
	if code := run([]string{"confirm-token", "--workspace=payments", "--action=purge", "--target=" + targetID.String()}, &out, &errBuf); code != 0 || !strings.Contains(out.String(), "tok-abc") {
		t.Fatalf("confirm-token failed: code=%d out=%s stderr=%s", code, out.String(), errBuf.String())
	}

	out.Reset()
	errBuf.Reset()
	if code := run([]string{"dead-letters", "--limit=15"}, &out, &errBuf); code != 0 || !strings.Contains(lastPath, "limit=15") {
		t.Fatalf("dead-letters failed: code=%d path=%q stderr=%s", code, lastPath, errBuf.String())
	}

	// Non-2xx returns exit code 1 cleanly.
	t.Setenv("MEM1_ADMIN_TOKEN", "wrong-token")
	out.Reset()
	errBuf.Reset()
	if code := run([]string{"dead-letters"}, &out, &errBuf); code != 1 || !strings.Contains(errBuf.String(), "HTTP 401") {
		t.Fatalf("expected HTTP 401 exit 1, code=%d stderr=%s", code, errBuf.String())
	}
}

func TestCLIEnvFileFlagAndMem1EnvFile(t *testing.T) {
	const adminToken = "envfile-admin-token"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+adminToken {
			http.Error(w, `{"error":"unauthorized","code":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(storage.DeadLettersReport{
			DeadLetters:   []storage.DeadLetterInboxItem{},
			DiscardedJobs: []storage.DiscardedRiverJob{},
		})
	}))
	defer srv.Close()

	envPath := filepath.Join(t.TempDir(), "mem1.env")
	envContent := "MEM1_HTTP_ADDR=" + srv.URL + "\nMEM1_ADMIN_TOKEN=" + adminToken + "\n"
	if err := os.WriteFile(envPath, []byte(envContent), 0o600); err != nil {
		t.Fatal(err)
	}

	var out, errBuf bytes.Buffer
	// --env-file=<path> before subcommand.
	if code := run([]string{"--env-file=" + envPath, "dead-letters"}, &out, &errBuf); code != 0 {
		t.Fatalf("--env-file= before subcommand failed: code=%d stderr=%s", code, errBuf.String())
	}

	// --env-file <path> after subcommand.
	out.Reset()
	errBuf.Reset()
	if code := run([]string{"dead-letters", "--env-file", envPath, "--limit=5"}, &out, &errBuf); code != 0 {
		t.Fatalf("--env-file after subcommand failed: code=%d stderr=%s", code, errBuf.String())
	}

	// MEM1_ENV_FILE environment variable.
	t.Setenv("MEM1_ENV_FILE", envPath)
	out.Reset()
	errBuf.Reset()
	if code := run([]string{"dead-letters"}, &out, &errBuf); code != 0 {
		t.Fatalf("MEM1_ENV_FILE failed: code=%d stderr=%s", code, errBuf.String())
	}

	// Missing --env-file value exits 2.
	for _, args := range [][]string{{"--env-file"}, {"--env-file="}, {"dead-letters", "--env-file"}} {
		out.Reset()
		errBuf.Reset()
		if code := run(args, &out, &errBuf); code != 2 {
			t.Fatalf("args %v expected exit 2, got %d (stderr=%s)", args, code, errBuf.String())
		}
	}

	// Nonexistent --env-file path exits 1.
	out.Reset()
	errBuf.Reset()
	missingPath := filepath.Join(t.TempDir(), "does-not-exist.env")
	if code := run([]string{"--env-file=" + missingPath, "dead-letters"}, &out, &errBuf); code != 1 {
		t.Fatalf("missing env file expected exit 1, got %d (stderr=%s)", code, errBuf.String())
	}
}
