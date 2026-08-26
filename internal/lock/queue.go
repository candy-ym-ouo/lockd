package lock

type waiterQueue struct {
	items []*waiter
}

func (q *waiterQueue) push(item *waiter) {
	q.items = append(q.items, item)
}
func (q *waiterQueue) pop() *waiter {
	for len(q.items) > 0 {
		item := q.items[0]
		q.items[0] = nil
		q.items = q.items[1:]
		if item == nil || item.cancelled {
			continue
		}
		return item
	}
	return nil
}
func (q *waiterQueue) remove(target *waiter) bool {
	for index, item := range q.items {
		if item != target {
			continue
		}
		copy(q.items[index:], q.items[index+1:])
		q.items[len(q.items)-1] = nil
		q.items = q.items[:len(q.items)-1]
		return true
	}
	return false
}
func (q *waiterQueue) activeLength() int {
	count := 0
	for _, item := range q.items {
		if item != nil && !item.cancelled {
			count++
		}
	}
	return count
}
func (q *waiterQueue) views() []WaiterView {
	result := make([]WaiterView, 0, len(q.items))
	for _, item := range q.items {
		if item != nil && !item.cancelled {
			result = append(result, item.view())
		}
	}
	return result
}
func (q *waiterQueue) cancelAll(err error) {
	for _, item := range q.items {
		if item == nil || item.cancelled || item.granted {
			continue
		}
		item.cancel()
		item.done <- acquireOutcome{err: err}
	}
	q.items = nil
}
