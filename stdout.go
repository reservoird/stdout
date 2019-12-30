package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/reservoird/icd"
)

// StdoutCfg contains config
type StdoutCfg struct {
	Name      string
	Timestamp bool
}

// StdoutStats contains stats
type StdoutStats struct {
	MessagesReceived uint64
	MessagesSent     uint64
	Running          bool
}

// Stdout contains what is needed for expeller
type Stdout struct {
	cfg       StdoutCfg
	run       bool
	statsChan chan StdoutStats
	clearChan chan struct{}
}

// New is what reservoird to create and start stdout
func New(cfg string) (icd.Expeller, error) {
	c := StdoutCfg{
		Name:      "com.reservoird.expel.stdout",
		Timestamp: false,
	}
	if cfg != "" {
		d, err := ioutil.ReadFile(cfg)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(d, &c)
		if err != nil {
			return nil, err
		}
	}
	o := &Stdout{
		cfg:       c,
		run:       true,
		statsChan: make(chan StdoutStats),
		clearChan: make(chan struct{}),
	}
	return o, nil
}

// Name provides name of expeller
func (o *Stdout) Name() string {
	return o.cfg.Name
}

// Stats return stats NOTE: thread safe
func (o *Stdout) Stats() (string, error) {
	select {
	case stats := <-o.statsChan:
		data, err := json.Marshal(stats)
		if err != nil {
			return "", err
		}
		return string(data), nil
	default:
	}
	return "", nil
}

// ClearStats clears stats NOTE: thread safe
func (o *Stdout) ClearStats() {
	select {
	case o.clearChan <- struct{}{}:
	default:
	}
}

// Expel reads messages from a channel and writes them to stdout
func (o *Stdout) Expel(queues []icd.Queue, done <-chan struct{}, wg *sync.WaitGroup) error {
	defer wg.Done()

	stats := StdoutStats{}

	o.run = true
	for o.run == true {
		for q := range queues {
			if queues[q].Closed() == false {
				d, err := queues[q].Get()
				if err != nil {
					fmt.Printf("%v\n", err)
				} else {
					if d != nil {
						stats.MessagesReceived = stats.MessagesReceived + 1
						data, ok := d.([]byte)
						if ok == false {
							fmt.Printf("error invalid type\n")
						} else {
							line := string(data)
							if o.cfg.Timestamp == true {
								line = fmt.Sprintf("[%s %s] ", o.Name(), time.Now().Format(time.RFC3339)) + line
							}
							fmt.Printf("%s", line)
							stats.MessagesSent = stats.MessagesSent + 1
						}
					}
				}
			}
		}

		select {
		case <-o.clearChan:
			stats = StdoutStats{}
		default:
		}

		select {
		case <-done:
			o.run = false
		default:
		}

		stats.Running = o.run
		select {
		case o.statsChan <- stats:
		default:
		}

		time.Sleep(time.Millisecond)
	}
	return nil
}
