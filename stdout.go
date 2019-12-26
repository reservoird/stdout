package main

import (
	"fmt"
	"sync"
)

type stdout struct {
	run bool
}

// Config configures consumer
func (o *stdout) Config(cfg string) error {
	return nil
}

// Expel reads messages from a channel and writes them to stdout
func (o *stdout) Expel(channel <-chan []byte, done <-chan struct{}, wg *sync.WaitGroup) error {
	defer wg.Done()
	o.run = true
	for o.run == true {
		line := <-channel
		fmt.Printf("%s", line)
		select {
		case <-done:
			o.run = false
		default:
		}

	}
	return nil
}

// Expeller for stdout
var Expeller stdout
