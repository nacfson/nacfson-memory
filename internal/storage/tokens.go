package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const defaultConfirmTokenTTL = 5 * time.Minute

func isValidConfirmAction(action string) bool {
	switch action {
	case "create", "edit", "promote", "revert", "purge":
		return true
	default:
		return false
	}
}

// IssueConfirmToken generates a random single-use confirmation token, stores its SHA-256
// digest in PostgreSQL, and returns the raw secret token plus its expiration timestamp.
func (s *Store) IssueConfirmToken(ctx context.Context, workspaceID, action string, targetMemoryID *uuid.UUID, ttl time.Duration) (string, time.Time, error) {
	return IssueConfirmToken(ctx, s.CurationPool, workspaceID, action, targetMemoryID, ttl)
}

// IssueConfirmToken stores a new single-use confirmation token using db.
func IssueConfirmToken(ctx context.Context, db DBTX, workspaceID, action string, targetMemoryID *uuid.UUID, ttl time.Duration) (string, time.Time, error) {
	if err := ValidateWorkspaceID(workspaceID); err != nil {
		return "", time.Time{}, err
	}
	if !isValidConfirmAction(action) {
		return "", time.Time{}, fmt.Errorf("invalid confirmation action %q: %w", action, ErrInvalidInput)
	}
	if ttl <= 0 {
		ttl = defaultConfirmTokenTTL
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}, fmt.Errorf("generate confirmation token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(token))
	expiresAt := time.Now().UTC().Add(ttl)

	// Opportunistically clean expired unconsumed tokens before inserting.
	_, _ = db.Exec(ctx, `DELETE FROM confirmation_tokens WHERE expires_at < now() AND consumed_at IS NULL`)

	_, err := db.Exec(ctx, `
		INSERT INTO confirmation_tokens (token_sha256, workspace_id, action, target_memory_id, expires_at)
		VALUES ($1, $2, $3, $4, $5)
	`, digest[:], workspaceID, action, targetMemoryID, expiresAt)
	if err != nil {
		if isForeignKeyViolation(err) {
			return "", time.Time{}, fmt.Errorf("workspace %q %w", workspaceID, ErrNotFound)
		}
		return "", time.Time{}, fmt.Errorf("insert confirmation token: %w", err)
	}
	return token, expiresAt, nil
}

// ConsumeConfirmToken atomically validates and marks a single-use confirmation token consumed.
func (s *Store) ConsumeConfirmToken(ctx context.Context, token, workspaceID, action string, targetMemoryID *uuid.UUID) error {
	return ConsumeConfirmToken(ctx, s.CurationPool, token, workspaceID, action, targetMemoryID)
}

// ConsumeConfirmToken atomically validates and marks a single-use confirmation token consumed using db.
func ConsumeConfirmToken(ctx context.Context, db DBTX, token, workspaceID, action string, targetMemoryID *uuid.UUID) error {
	if token == "" || !isValidConfirmAction(action) || ValidateWorkspaceID(workspaceID) != nil {
		return ErrInvalidToken
	}
	digest := sha256.Sum256([]byte(token))

	var consumedDigest []byte
	err := db.QueryRow(ctx, `
		UPDATE confirmation_tokens
		SET consumed_at = now()
		WHERE token_sha256 = $1
		  AND workspace_id = $2
		  AND action = $3
		  AND consumed_at IS NULL
		  AND expires_at > now()
		  AND (($4::uuid IS NULL AND target_memory_id IS NULL) OR target_memory_id = $4)
		RETURNING token_sha256
	`, digest[:], workspaceID, action, targetMemoryID).Scan(&consumedDigest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrInvalidToken
		}
		return fmt.Errorf("consume confirmation token: %w", err)
	}
	return nil
}
