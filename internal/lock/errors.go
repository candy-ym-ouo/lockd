package lock

import "fmt"

type ErrorCode int

const (
	CodeInvalid           ErrorCode = 10001
	CodeNotFound          ErrorCode = 10002
	CodeAlreadyExists     ErrorCode = 10003
	CodeLocked            ErrorCode = 10004
	CodeTokenInvalid      ErrorCode = 10005
	CodeNotHeld           ErrorCode = 10006
	CodeWaitTimeout       ErrorCode = 10007
	CodeNamespaceDenied   ErrorCode = 10008
	CodeQuotaExceeded     ErrorCode = 10009
	CodeForceUnauthorized ErrorCode = 10010
	CodeInternal          ErrorCode = 10011
)

type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("lockd: %d %s", e.Code, e.Message) }
func coreError(code ErrorCode, message string) error {
	return &Error{Code: code, Message: message}
}

var (
	ErrInvalid           = coreError(CodeInvalid, "invalid argument")
	ErrNotFound          = coreError(CodeNotFound, "lock not found")
	ErrAlreadyExists     = coreError(CodeAlreadyExists, "lock already exists")
	ErrLocked            = coreError(CodeLocked, "lock is held")
	ErrTokenInvalid      = coreError(CodeTokenInvalid, "token is invalid")
	ErrNotHeld           = coreError(CodeNotHeld, "lock is not held")
	ErrWaitTimeout       = coreError(CodeWaitTimeout, "wait timed out")
	ErrNamespaceDenied   = coreError(CodeNamespaceDenied, "namespace is not allowed")
	ErrQuotaExceeded     = coreError(CodeQuotaExceeded, "namespace quota exceeded")
	ErrForceUnauthorized = coreError(CodeForceUnauthorized, "force token is invalid")
)
