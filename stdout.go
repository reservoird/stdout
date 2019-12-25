package main

import (
	"fmt"
)

type stdout struct {
}

// Config configures consumer
func (o *stdout) Config(cfg string) error {
	return nil
}

// Expel reads messages from a channel and writes them to stdout
func (o *stdout) Expel(channel <-chan []byte) error {
	for {
		line := <-channel
		fmt.Sprintf("%s", line)
	}
}

// Expeller for stdout
var Expeller stdout
