package lock

import (
	"sync"
	"time"
)

const (
	EventHeld     = "held"
	EventReleased = "released"
	EventExpired  = "expired"
	EventStolen   = "stolen"
	EventPurged   = "purged"
)

type CreateOptions struct {
	Namespace   string
	Name        string
	Reentrant   bool
	MaxDepth    int
	DefaultTTL  time.Duration
	AutoCleanup time.Duration
}

type AcquireOptions struct {
	Holder    string
	RequestID string
	TTL       time.Duration
	Wait      bool
}

type AcquireResult struct {
	Token     string    `json:"token"`
	Holder    string    `json:"holder"`
	Depth     int       `json:"depth"`
	ExpiresAt time.Time `json:"expires_at"`
	Reentered bool      `json:"reentered"`
}

type ReleaseResult struct {
	Released   bool   `json:"released"`
	Depth      int    `json:"depth"`
	NextHolder string `json:"next_holder"`
}

type Event struct {
	Lock      string    `json:"lock"`
	Namespace string    `json:"namespace"`
	Event     string    `json:"event"`
	Reason    string    `json:"reason"`
	At        time.Time `json:"at"`
	Seq       uint64    `json:"seq"`
}

type WaiterView struct {
	Seq          uint64    `json:"seq"`
	Holder       string    `json:"holder"`
	RequestID    string    `json:"request_id,omitempty"`
	WaitingSince time.Time `json:"waiting_since"`
}

type View struct {
	FullName    string       `json:"full_name"`
	Namespace   string       `json:"namespace"`
	Name        string       `json:"name"`
	CreatedAt   time.Time    `json:"created_at"`
	Reentrant   bool         `json:"reentrant"`
	MaxDepth    int          `json:"max_depth"`
	DefaultTTL  string       `json:"default_ttl"`
	State       string       `json:"state"`
	Holder      string       `json:"holder,omitempty"`
	Depth       int          `json:"depth"`
	TokenHint   string       `json:"token_hint,omitempty"`
	ExpiresAt   *time.Time   `json:"expires_at,omitempty"`
	QueueLength int          `json:"queue_length"`
	Queue       []WaiterView `json:"queue,omitempty"`
	Version     uint64       `json:"version"`
	LastIdleAt  time.Time    `json:"last_idle_at"`
}
type record struct {
	mu            sync.Mutex
	namespace     string
	name          string
	createdAt     time.Time
	reentrant     bool
	maxDepth      int
	defaultTTL    time.Duration
	autoCleanup   time.Duration
	holder        string
	token         string
	depth         int
	expiresAt     time.Time
	version       uint64
	lastIdleAt    time.Time
	deleted       bool
	queue         waiterQueue
	queueBuffer   []WaiterView
	releasedToken map[string]struct{}
}

func (r *record) fullName() string { return r.namespace + ":" + r.name }
func (r *record) held(now time.Time) bool {
	return r.token != "" && now.Before(r.expiresAt)
}
func (r *record) snapshot(now time.Time) View {
	state := "idle"
	var expires *time.Time
	if r.held(now) {
		state = "held"
		copyTime := r.expiresAt
		expires = &copyTime
	}
	r.queueBuffer = r.queue.viewsInto(r.queueBuffer[:0])
	queue := r.queueBuffer
	return View{
		FullName:    r.fullName(),
		Namespace:   r.namespace,
		Name:        r.name,
		CreatedAt:   r.createdAt,
		Reentrant:   r.reentrant,
		MaxDepth:    r.maxDepth,
		DefaultTTL:  r.defaultTTL.String(),
		State:       state,
		Holder:      r.holder,
		Depth:       r.depth,
		TokenHint:   tokenHint(r.token),
		ExpiresAt:   expires,
		QueueLength: len(queue),
		Queue:       queue,
		Version:     r.version,
		LastIdleAt:  r.lastIdleAt,
	}
}
func tokenHint(token string) string {
	if len(token) <= 10 {
		return token
	}
	return token[:7] + "…" + token[len(token)-4:]
}
