package goku

import (
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

var (
	ErrUpdateRace  = errors.New("update failed due to data race", j.C("ERR_021c218d3d627915")) // Concurrent sets can cause race errors, can just try again.
	ErrNotFound    = errors.New("key not found", j.C("ERR_1c1777690f774c97"))
	ErrInvalidKey  = errors.New("invalid key", j.C("ERR_75bca259ff56586e"))
	ErrConditional = errors.New("conditional update failed", j.C("ERR_3a315c1fe3a73d55"))
)
