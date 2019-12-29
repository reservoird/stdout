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
	stats     chan<- string
	run       bool
	Tag       string
	Timestamp bool
}

// New is what reservoird to create and start stdout
func New(cfg string, stats chan<- string) (icd.Expeller, error) {
	o := &stdout{
		stats:     stats,
		run:       true,
		Tag:       "stdout",
		Timestamp: false,
	}
	if cfg != "" {
		d, err := ioutil.ReadFile(cfg)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(d, o)
		if err != nil {
			return nil, err
		}
	}
	return o, nil
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
			time.Sleep(time.Millisecond)
		}
	}
	return nil
}
