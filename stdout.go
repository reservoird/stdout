package main

import (
	"fmt"
	"sync"

	"github.com/reservoird/icd"
)

type stdout struct {
	run bool
}

// Config configures consumer
func (o *stdout) Config(cfg string) error {
	return nil
}

// NewExpeller is what reservoird to create and start stdout
func NewExpeller() (icd.Expeller, error) {
	return new(stdout), nil
}

// Expel reads messages from a channel and writes them to stdout
func (o *stdout) Expel(queues []icd.Queue, done <-chan struct{}, wg *sync.WaitGroup) error {
	defer wg.Done()
	o.run = true
	for o.run == true {
		for q := range queues {
			d, err := queues[q].Pop()
			if err != nil {
				return err
			}
			line, ok := d.([]byte)
			if ok == false {
				return fmt.Errorf("error invalid type")
			}
			fmt.Printf("%s", line)
		}
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
