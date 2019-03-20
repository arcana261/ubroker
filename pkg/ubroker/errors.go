package ubroker

import "errors"

var (
	ErrInvalidID     = errors.New("id is invalid")
	ErrUnimplemented = errors.New("method is not implemented")
	ErrClosed        = errors.New("closed")
)
