package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/reservoird/icd"
)

type stdout struct {
	run       bool
	Tag       string
	Timestamp bool
}

// NewExpeller is what reservoird to create and start stdout
func NewExpeller() (icd.Expeller, error) {
	return new(stdout), nil
}

// Config configures consumer
func (o *stdout) Config(cfg string) error {
	o.Tag = "stdout"
	o.Timestamp = false
	if cfg != "" {
		d, err := ioutil.ReadFile(cfg)
		if err != nil {
			return err
		}
		s := stdout{}
		err = json.Unmarshal(d, &s)
		if err != nil {
			return err
		}
		o.Tag = s.Tag
		o.Timestamp = s.Timestamp
	}
	return nil
}

// Name provides name of expeller
func (o *stdout) Name() string {
	return o.Tag
}

// Expel reads messages from a channel and writes them to stdout
func (o *stdout) Expel(queues []icd.Queue, done <-chan struct{}, wg *sync.WaitGroup) error {
	defer wg.Done()

	o.run = true
	for o.run == true {
		for q := range queues {
			d, err := queues[q].Get()
			if err != nil {
				fmt.Printf("%v\n", err)
			} else {
				if d != nil {
					data, ok := d.([]byte)
					if ok == false {
						fmt.Printf("error invalid type\n")
					} else {
						line := string(data)
						if o.Timestamp == true {
							line = fmt.Sprintf("[%s %s] ", o.Name(), time.Now().Format(time.RFC3339)) + line
						}
						fmt.Printf("%s", line)
					}
				}
			}
		}

		select {
		case <-done:
			o.run = false
		default:
			time.Sleep(time.Second)
		}
	}
	return nil
}
