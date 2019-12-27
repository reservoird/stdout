package main

import (
	"fmt"
	"sync"

	"github.com/reservoird/reservoird/run"
)

type stdout struct {
	run bool
}

// Config configures consumer
func (o *stdout) Config(cfg string) error {
	return nil
}

// Expel reads messages from a channel and writes them to stdout
func (o *stdout) Expel(queue run.Queue, done <-chan struct{}, wg *sync.WaitGroup) error {
	defer wg.Done()
	o.run = true
	for o.run == true {
		d, err := queue.Pop()
		if err != nil {
			return err
		}
		line, ok := d.([]byte)
		if ok == false {
			return fmt.Errorf("error invalid type")
		}
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
