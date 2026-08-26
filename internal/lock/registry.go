package lock

import (
	"regexp"
	"sort"
	"sync"
	"time"
)

var validName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

type Registry struct {
	mu         sync.RWMutex
	locks      map[string]*record
	allowed    map[string]struct{}
	quota      int
	nsCounts   map[string]int
	defaultTTL time.Duration
	listBuffer []View
}

func NewRegistry(namespaces []string, quota int, defaultTTL time.Duration) *Registry {
	allowed := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		allowed[namespace] = struct{}{}
	}
	if quota <= 0 {
		quota = 100000
	}
	if defaultTTL <= 0 {
		defaultTTL = DefaultTTL
	}
	return &Registry{
		locks:      make(map[string]*record),
		allowed:    allowed,
		quota:      quota,
		nsCounts:   make(map[string]int),
		defaultTTL: defaultTTL,
	}
}
func fullName(namespace, name string) string { return namespace + ":" + name }
func (r *Registry) namespaceAllowed(namespace string) bool {
	if len(r.allowed) == 0 {
		return true
	}
	_, ok := r.allowed[namespace]
	return ok
}

func (r *Registry) Create(options CreateOptions) (View, error) {
	if !validName.MatchString(options.Namespace) || !validName.MatchString(options.Name) {
		return View{}, &Error{Code: CodeInvalid, Message: "namespace and name must match ^[a-zA-Z0-9_.-]{1,64}$"}
	}
	if !r.namespaceAllowed(options.Namespace) {
		return View{}, ErrNamespaceDenied
	}
	ttl, err := normalizeTTL(options.DefaultTTL, r.defaultTTL)
	if err != nil {
		return View{}, err
	}
	if options.MaxDepth == 0 {
		options.MaxDepth = 64
	}
	if options.MaxDepth < 1 || options.MaxDepth > 1024 {
		return View{}, &Error{Code: CodeInvalid, Message: "max_depth must be between 1 and 1024"}
	}
	now := time.Now().UTC()
	item := &record{
		namespace:     options.Namespace,
		name:          options.Name,
		createdAt:     now,
		reentrant:     options.Reentrant,
		maxDepth:      options.MaxDepth,
		defaultTTL:    ttl,
		autoCleanup:   options.AutoCleanup,
		lastIdleAt:    now,
		releasedToken: make(map[string]struct{}),
	}
	key := fullName(options.Namespace, options.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.locks[key]; exists {
		return View{}, ErrAlreadyExists
	}
	if r.nsCounts[options.Namespace] >= r.quota {
		return View{}, ErrQuotaExceeded
	}
	r.locks[key] = item
	r.nsCounts[options.Namespace]++
	return item.snapshot(now), nil
}
func (r *Registry) get(namespace, name string) (*record, error) {
	if !r.namespaceAllowed(namespace) {
		return nil, ErrNamespaceDenied
	}
	r.mu.RLock()
	item := r.locks[fullName(namespace, name)]
	r.mu.RUnlock()
	if item == nil {
		return nil, ErrNotFound
	}
	return item, nil
}

func (r *Registry) Get(namespace, name string) (View, error) {
	item, err := r.get(namespace, name)
	if err != nil {
		return View{}, err
	}
	now := time.Now().UTC()
	item.mu.Lock()
	defer item.mu.Unlock()
	// FIX: get may have observed the record immediately before a concurrent delete.
	if item.deleted {
		return View{}, ErrNotFound
	}
	return item.snapshot(now), nil
}

func (r *Registry) List(namespace, state string) []View {
	now := time.Now().UTC()
	r.mu.RLock()
	items := make([]*record, 0, len(r.locks))
	for _, item := range r.locks {
		if namespace == "" || item.namespace == namespace {
			items = append(items, item)
		}
	}
	r.mu.RUnlock()
	r.mu.RLock()
	views := r.listBuffer[:0]
	r.mu.RUnlock()
	for _, item := range items {
		item.mu.Lock()
		if item.deleted {
			item.mu.Unlock()
			continue
		}
		view := item.snapshot(now)
		item.mu.Unlock()
		if state == "" || state == "all" || view.State == state {
			views = append(views, view)
		}
	}
	sort.Slice(views, func(i, j int) bool { return views[i].FullName < views[j].FullName })
	r.mu.Lock()
	r.listBuffer = views
	r.mu.Unlock()
	return views
}
func (r *Registry) allRecords() []*record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := make([]*record, 0, len(r.locks))
	for _, item := range r.locks {
		items = append(items, item)
	}
	return items
}
func (r *Registry) remove(item *record) bool {
	key := item.fullName()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.locks[key] != item {
		return false
	}
	delete(r.locks, key)
	r.nsCounts[item.namespace]--
	return true
}

func (r *Registry) Stats() (int, int, map[string]int) {
	views := r.List("", "all")
	waiters := 0
	byNamespace := make(map[string]int)
	for _, view := range views {
		waiters += view.QueueLength
		byNamespace[view.Namespace]++
	}
	return len(views), waiters, byNamespace
}
