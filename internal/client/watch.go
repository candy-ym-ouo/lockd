package client

import (
	"context"
	"time"
)

type Event struct {
	Lock      string    `json:"lock"`
	Namespace string    `json:"namespace"`
	Event     string    `json:"event"`
	Reason    string    `json:"reason"`
	At        time.Time `json:"at"`
	Seq       uint64    `json:"seq"`
}

func (c *Client) Watch(ctx context.Context, namespace, name string, timeout time.Duration) (Event, error) {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	body := map[string]string{"timeout": timeout.String()}
	var event Event
	err := c.do(ctx, "POST", lockPath(namespace, name)+"/watch", body, "", &event)
	return event, err
}

func (c *Client) WatchLoop(ctx context.Context, namespace, name string, callback func(Event)) error {
	for {
		event, err := c.Watch(ctx, namespace, name, 60*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if apiErr, ok := err.(*Error); ok && apiErr.Code == 10007 {
				continue
			}
			return err
		}
		if callback != nil {
			callback(event)
		}
	}
}
