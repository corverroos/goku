package goku

import "github.com/luno/jettison/errors"

var (
	ErrSetRace  = errors.New("set failed due to data race") // Concurrent sets can cause race errors, can just try again.
	ErrNotFound = errors.New("key not found")
)
