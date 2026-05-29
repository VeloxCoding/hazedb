package hazedb

import "errors"

var (
	ErrDuplicatePK    = errors.New("fastsql: duplicate primary key")
	ErrUnknownTable   = errors.New("fastsql: unknown table")
	ErrUnknownColumn  = errors.New("fastsql: unknown column")
	ErrTypeMismatch   = errors.New("fastsql: type mismatch")
	ErrParamMismatch  = errors.New("fastsql: parameter count mismatch")
	ErrPKUpdate       = errors.New("fastsql: UPDATE on PK column not supported")
	ErrParse          = errors.New("fastsql: parse error")
	ErrWALCorrupt     = errors.New("fastsql: WAL corrupted")
	ErrTableExists    = errors.New("hazedb: table already exists")
)
