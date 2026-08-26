package api

import (
	"context"
	"encoding/json"
	core "lockd/internal/lock"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type createRequest struct {
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	Reentrant   *bool  `json:"reentrant"`
	MaxDepth    int    `json:"max_depth"`
	TTL         string `json:"ttl"`
	AutoCleanup string `json:"auto_cleanup"`
}
type acquireRequest struct {
	Holder      string `json:"holder"`
	RequestID   string `json:"request_id"`
	TTL         string `json:"ttl"`
	Wait        bool   `json:"wait"`
	WaitTimeout string `json:"wait_timeout"`
}
type renewRequest struct {
	Token string `json:"token"`
	TTL   string `json:"ttl"`
}
type releaseRequest struct {
	Token     string `json:"token"`
	RequestID string `json:"request_id"`
}
type watchRequest struct {
	Holder    string `json:"holder"`
	RequestID string `json:"request_id"`
	Timeout   string `json:"timeout"`
}
type stealRequest struct {
	Holder string `json:"holder"`
	TTL    string `json:"ttl"`
	Reason string `json:"reason"`
}

func (a *API) handleLocks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		a.createLock(w, r)
	case http.MethodGet:
		a.listLocks(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
func (a *API) createLock(w http.ResponseWriter, r *http.Request) {
	var request createRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	ttl, err := parseOptionalDuration(request.TTL)
	if err != nil {
		writeError(w, err)
		return
	}
	cleanup, err := parseOptionalDuration(request.AutoCleanup)
	if err != nil {
		writeError(w, err)
		return
	}
	reentrant := true
	if request.Reentrant != nil {
		reentrant = *request.Reentrant
	}
	view, err := a.service.Create(core.CreateOptions{
		Namespace:   request.Namespace,
		Name:        request.Name,
		Reentrant:   reentrant,
		MaxDepth:    request.MaxDepth,
		DefaultTTL:  ttl,
		AutoCleanup: cleanup,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, map[string]any{"full_name": view.FullName, "created_at": view.CreatedAt})
}
func (a *API) listLocks(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	views := a.service.List(query.Get("namespace"), query.Get("state"))
	offset, _ := strconv.Atoi(query.Get("offset"))
	limit, _ := strconv.Atoi(query.Get("limit"))
	if offset < 0 {
		offset = 0
	}
	if offset >= len(views) {
		views = []core.View{}
	} else if offset > 0 {
		views = views[offset:]
	}
	if limit > 0 && limit < len(views) {
		views = views[:limit]
	}
	for index := range views {
		for waiter := range views[index].Queue {
			views[index].Queue[waiter].RequestID = ""
		}
	}
	writeJSON(w, http.StatusOK, views)
}
func (a *API) handleLockPath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/locks/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, core.ErrNotFound)
		return
	}
	namespace, name := parts[0], parts[1]
	if len(parts) == 2 {
		switch r.Method {
		case http.MethodGet:
			view, err := a.service.Get(namespace, name)
			if err != nil {
				writeError(w, err)
				return
			}
			writeOK(w, view)
		case http.MethodDelete:
			err := a.service.Delete(namespace, name, r.Header.Get("X-Force-Token"))
			if err != nil {
				writeError(w, err)
				return
			}
			writeOK(w, map[string]bool{"deleted": true})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}
	if r.Method != http.MethodPost || len(parts) != 3 {
		writeError(w, core.ErrNotFound)
		return
	}
	switch parts[2] {
	case "acquire":
		a.acquire(w, r, namespace, name)
	case "renew":
		a.renew(w, r, namespace, name)
	case "release":
		a.release(w, r, namespace, name)
	case "watch":
		a.watch(w, r, namespace, name)
	case "steal":
		a.steal(w, r, namespace, name)
	default:
		writeError(w, core.ErrNotFound)
	}
}
func (a *API) acquire(w http.ResponseWriter, r *http.Request, namespace, name string) {
	var request acquireRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	ttl, err := parseOptionalDuration(request.TTL)
	if err != nil {
		writeError(w, err)
		return
	}
	ctx := r.Context()
	if request.Wait {
		// FIX: wait_timeout=0 still obeys the documented server-side 10 minute cap.
		timeout := 10 * time.Minute
		if request.WaitTimeout != "" {
			parsed, parseErr := time.ParseDuration(request.WaitTimeout)
			if parseErr != nil || parsed < 0 || parsed > 5*time.Minute {
				writeError(w, &core.Error{Code: core.CodeInvalid, Message: "invalid wait_timeout"})
				return
			}
			if parsed > 0 {
				timeout = parsed
			}
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	result, err := a.service.Acquire(ctx, namespace, name, core.AcquireOptions{
		Holder: request.Holder, RequestID: request.RequestID, TTL: ttl, Wait: request.Wait,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, result)
}
func (a *API) renew(w http.ResponseWriter, r *http.Request, namespace, name string) {
	var request renewRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	ttl, err := parseOptionalDuration(request.TTL)
	if err != nil {
		writeError(w, err)
		return
	}
	expiresAt, err := a.service.Renew(namespace, name, request.Token, ttl)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, map[string]any{"expires_at": expiresAt})
}
func (a *API) release(w http.ResponseWriter, r *http.Request, namespace, name string) {
	var request releaseRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	result, err := a.service.Release(namespace, name, request.Token)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, result)
}
func (a *API) watch(w http.ResponseWriter, r *http.Request, namespace, name string) {
	var request watchRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	timeout := 60 * time.Second
	if request.Timeout != "" {
		parsed, err := time.ParseDuration(request.Timeout)
		if err != nil || parsed <= 0 || parsed > 10*time.Minute {
			writeError(w, &core.Error{Code: core.CodeInvalid, Message: "invalid timeout"})
			return
		}
		timeout = parsed
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	event, err := a.service.Watch(ctx, namespace, name)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, event)
}
func (a *API) steal(w http.ResponseWriter, r *http.Request, namespace, name string) {
	var request stealRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	ttl, err := parseOptionalDuration(request.TTL)
	if err != nil {
		writeError(w, err)
		return
	}
	result, err := a.service.Steal(namespace, name, r.Header.Get("X-Force-Token"), request.Holder, ttl)
	if err != nil {
		writeError(w, err)
		return
	}
	writeOK(w, result)
}
func parseOptionalDuration(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		return 0, &core.Error{Code: core.CodeInvalid, Message: "invalid duration"}
	}
	return parsed, nil
}
func encodeSSE(w http.ResponseWriter, event string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("event: " + event + "\ndata: " + string(data) + "\n\n"))
	return err
}
