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
			close(channel)
		})
	}
	return channel, cancel
}

func (b *Bus) Broadcast(event Event) int {
	b.mu.RLock()
	channels := make([]chan Event, 0, len(b.subs))
	for _, channel := range b.subs {
		channels = append(channels, channel)
	}
	b.mu.RUnlock()
	delivered := 0
	for _, channel := range channels {
		select {
		case channel <- event:
			delivered++
		default:
		}
	}
	return delivered
}
