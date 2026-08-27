package lock

import (
	"sync"
	"testing"
)

// Reproduces "send on closed channel" by racing cancel()/close against Broadcast.
func TestRaceBroadcastVsCancel(t *testing.T) {
	for round := 0; round < 50; round++ {
		bus := NewBus()
		// A long-lived subscriber that stays in the map.
		keepCh, keepCancel := bus.Subscribe(16)
		defer keepCancel()

		var wg sync.WaitGroup
		const flashers = 8
		wg.Add(flashers)
		for i := 0; i < flashers; i++ {
			go func() {
				defer wg.Done()
				_, cancel := bus.Subscribe(1)
				// connect-then-immediately-disconnect
				cancel()
			}()
		}
		// Broadcast while flashers are subscribing+cancelling.
		bcast := func() {
			for i := 0; i < 200; i++ {
				bus.Broadcast(Event{Event: EventHeld})
			}
		}
		wg.Add(1)
		go func() { defer wg.Done(); bcast() }()
		wg.Wait()
		// drain
		for len(keepCh) > 0 {
			<-keepCh
		}
	}
}
