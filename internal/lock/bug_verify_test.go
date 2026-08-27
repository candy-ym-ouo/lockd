package lock

import (
	"fmt"
	"sync"
	"testing"
)

func TestBug06_SubscriptionCancelDuringBroadcastIsSafe(t *testing.T) {
	for round := 0; round < 40; round++ {
		bus := NewBus()
		cancels := make([]func(), 256)
		for index := range cancels {
			_, cancels[index] = bus.Subscribe(1)
		}
		start := make(chan struct{})
		panics := make(chan any, 8)
		var wg sync.WaitGroup
		for worker := 0; worker < 4; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if recovered := recover(); recovered != nil {
						panics <- recovered
					}
				}()
				<-start
				bus.Broadcast(Event{Event: EventHeld})
			}()
		}
		for worker := 0; worker < 8; worker++ {
			worker := worker
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for index := worker; index < len(cancels); index += 8 {
					cancels[index]()
				}
			}()
		}
		close(start)
		wg.Wait()
		select {
		case recovered := <-panics:
			t.Fatal(fmt.Sprint(recovered))
		default:
		}
	}
}
