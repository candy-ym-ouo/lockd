package api

import (
	"encoding/json"
	"errors"
	"io"
	core "lockd/internal/lock"
	"net/http"
)

type envelope struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data any    `json:"data,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeOK(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, envelope{Code: 0, Msg: "ok", Data: data})
}
func writeError(w http.ResponseWriter, err error) {
	var lockErr *core.Error
	if !errors.As(err, &lockErr) {
		lockErr = &core.Error{Code: core.CodeInternal, Message: "internal error"}
	}
	status := http.StatusInternalServerError
	switch lockErr.Code {
	case core.CodeInvalid:
		status = http.StatusBadRequest
	case core.CodeNotFound:
		status = http.StatusNotFound
	case core.CodeAlreadyExists, core.CodeLocked, core.CodeNotHeld:
		status = http.StatusConflict
	case core.CodeTokenInvalid, core.CodeNamespaceDenied, core.CodeForceUnauthorized:
		status = http.StatusForbidden
	case core.CodeWaitTimeout:
		status = http.StatusRequestTimeout
	case core.CodeQuotaExceeded:
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, envelope{Code: int(lockErr.Code), Msg: lockErr.Message})
}
func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return &core.Error{Code: core.CodeInvalid, Message: "invalid JSON body"}
	}
	// FIX: reject a second JSON value instead of silently accepting trailing input.
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return &core.Error{Code: core.CodeInvalid, Message: "request body must contain one JSON value"}
	}
	return nil
}
