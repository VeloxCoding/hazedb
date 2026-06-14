package hazedb

import "errors"

var (
	ErrDuplicatePK       = errors.New("hazedb: duplicate primary key")
	ErrUnknownTable      = errors.New("hazedb: unknown table")
	ErrUnknownColumn     = errors.New("hazedb: unknown column")
	ErrTypeMismatch      = errors.New("hazedb: type mismatch")
	ErrParamMismatch     = errors.New("hazedb: parameter count mismatch")
	ErrPKUpdate          = errors.New("hazedb: UPDATE on PK column not supported")
	ErrParse             = errors.New("hazedb: parse error")
	ErrUnindexedJoin     = errors.New("hazedb: JOIN requires an index on the join column")
	ErrWALCorrupt        = errors.New("hazedb: WAL corrupted")
	ErrWALVersion        = errors.New("hazedb: WAL version mismatch")
	ErrTableExists       = errors.New("hazedb: table already exists")
	ErrTxUnsupported     = errors.New("hazedb: operation not supported in a transaction")
	ErrBatchTooLarge     = errors.New("hazedb: atomic batch too large")
	ErrCapacity          = errors.New("hazedb: store byte capacity exceeded")
	ErrReservedName      = errors.New("hazedb: table name uses the reserved _hz_ prefix")
	ErrCompanionInMemory = errors.New("hazedb: companion must be an on-disk file, not in-memory")
	ErrClosed            = errors.New("hazedb: database is closed")
	ErrMirrorSchema      = errors.New("hazedb: mirror schema mismatch; migration required")
	ErrMirrorCorrupt     = errors.New("hazedb: SQLite mirror corrupted")
)
