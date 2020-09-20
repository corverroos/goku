package goku

import "github.com/luno/jettison/errors"

var (
	ErrUpdateRace = errors.New("update failed due to data race") // Concurrent sets can cause race errors, can just try again.
	ErrNotFound   = errors.New("key not found")
	ErrInvalidKey   = errors.New("invalid key")
)
