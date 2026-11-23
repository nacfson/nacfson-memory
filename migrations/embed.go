package migrations

import "embed"

// FS holds embedded SQL migrations for mem1.
//
//go:embed *.sql
var FS embed.FS
