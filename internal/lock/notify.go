package lock

import "sync"

type Bus struct {
	mu     sync.RWMutex
	nextID uint64
	subs   map[uint64]chan Event
}

func NewBus() *Bus {
	return &Bus{subs: make(map[uint64]chan Event)}
}

func (b *Bus) Subscribe(buffer int) (<-chan Event, func()) {
	if buffer < 1 {
		buffer = 1
	}
	b.mu.Lock()
	b.nextID++
	id := b.nextID
	channel := make(chan Event, buffer)
	b.subs[id] = channel
	b.mu.Unlock()
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, id)
			b.mu.Unlock()
		})
	}
	return channel, cancel
}

func (b *Bus) Broadcast(event Event) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	delivered := 0
	for _, channel := range b.subs {
		select {
		case channel <- event:
			delivered++
		default:
		}
	}
	return delivered
}
