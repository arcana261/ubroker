package ubroker

import "errors"

var (
	// ErrInvalidID is returned only in case when
	// an `id` is supplied to acknowledge/requeue which
	// does not exist or has not been assigned yet.
	ErrInvalidID = errors.New("id is invalid")

	// ErrUnimplemented is the default error returned
	// by yet to be implemented methods of broker!
	ErrUnimplemented = errors.New("method is not implemented")

	// ErrClosed is returned by broker methods when
	// servicer has been shutted-down and a new request
	// rolls in
	ErrClosed = errors.New("closed")
)