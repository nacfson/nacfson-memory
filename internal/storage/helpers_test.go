//go:build integration

package storage_test

import "github.com/jackc/pgx/v5"

func pgxTxReadOnly() pgx.TxOptions {
	return pgx.TxOptions{AccessMode: pgx.ReadOnly}
}
