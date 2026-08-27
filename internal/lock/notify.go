package lock

import "sync"

type Bus struct {
	mu     sync.Mutex
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
			// Remove the subscriber and close its channel under the write
			// lock. Broadcast also holds this lock for the whole send loop,
			// so a send and a close can never overlap on the same channel —
			// closing is serialized either before Broadcast sees the channel
			// (it is gone from the map) or after Broadcast has finished
			// sending (closing an already-sent channel is a no-op for senders).
			b.mu.Lock()
			delete(b.subs, id)
			close(channel)
			b.mu.Unlock()
		})
	}
	return channel, cancel
}

func (b *Bus) Broadcast(event Event) int {
	// Hold the write lock across the whole send loop. Sends are non-blocking
	// (the default clause skips a full buffer), so a slow subscriber cannot
	// stall the bus, and because cancel() takes the same lock to close a
	// channel, a send and a close can never race on the same channel.
	b.mu.Lock()
	defer b.mu.Unlock()
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
