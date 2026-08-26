package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"lockd/internal/client"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx := context.Background()
	command := os.Args[1]
	set := flag.NewFlagSet(command, flag.ExitOnError)
	server := set.String("server", "http://127.0.0.1:8080", "lockd server URL")
	namespace := set.String("namespace", "default", "lock namespace")
	name := set.String("name", "", "lock name")
	holder := set.String("holder", "lockctl", "holder identity")
	token := set.String("token", "", "lease token")
	ttl := set.Duration("ttl", 30*time.Second, "lease TTL")
	wait := set.Bool("wait", false, "wait for acquisition")
	waitTimeout := set.Duration("wait-timeout", 10*time.Second, "wait timeout")
	forceToken := set.String("force-token", "", "administrator force token")
	_ = set.Parse(os.Args[2:])
	c := client.New(*server)
	var output any
	var err error
	switch command {
	case "create":
		err = requireName(*name)
		if err == nil {
			err = c.Create(ctx, *namespace, *name, true, *ttl)
		}
		output = map[string]any{"created": err == nil, "lock": *namespace + ":" + *name}
	case "list":
		output, err = c.List(ctx, *namespace)
	case "acquire":
		err = requireName(*name)
		if err == nil {
			output, err = c.Acquire(ctx, *namespace, *name, *holder, client.AcquireOptions{
				TTL: *ttl, Wait: *wait, WaitTimeout: *waitTimeout,
			})
		}
	case "renew":
		err = requireLease(*name, *token)
		lease := &client.Lease{Namespace: *namespace, Name: *name, Token: *token, TTL: *ttl}
		if err == nil {
			err = c.Renew(ctx, lease)
			output = lease
		}
	case "release":
		err = requireLease(*name, *token)
		lease := &client.Lease{Namespace: *namespace, Name: *name, Token: *token, TTL: *ttl}
		if err == nil {
			err = c.Release(ctx, lease)
		}
		output = map[string]bool{"released": err == nil}
	case "watch":
		err = requireName(*name)
		if err == nil {
			output, err = c.Watch(ctx, *namespace, *name, *waitTimeout)
		}
	case "steal":
		err = requireName(*name)
		if err == nil {
			output, err = c.Steal(ctx, *namespace, *name, *holder, *ttl, *forceToken)
		}
	case "delete":
		err = requireName(*name)
		if err == nil {
			err = c.Delete(ctx, *namespace, *name, *forceToken)
		}
		output = map[string]bool{"deleted": err == nil}
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	data, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(data))
}
func requireName(name string) error {
	if name == "" {
		return fmt.Errorf("-name is required")
	}
	return nil
}
func requireLease(name, token string) error {
	if err := requireName(name); err != nil {
		return err
	}
	if token == "" {
		return fmt.Errorf("-token is required")
	}
	return nil
}
func usage() {
	fmt.Fprintln(os.Stderr, "usage: lockctl <create|list|acquire|renew|release|watch|steal|delete> [flags]")
}
