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
	Monitoring       bool
}

// Stdout contains what is needed for expeller
type Stdout struct {
	cfg       StdoutCfg
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
		statsChan: make(chan StdoutStats),
		clearChan: make(chan struct{}),
	}
	return o, nil
}

// Name provides name of expeller
func (o *Stdout) Name() string {
	return o.cfg.Name
}

// Monitor provides statistics and clear
func (o *Stdout) Monitor(statsChan chan<- string, clearChan <-chan struct{}, doneChan <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done() // required

	stats := StdoutStats{}
	monrun := true
	for monrun == true {
		// clear
		select {
		case <-clearChan:
			select {
			case o.clearChan <- struct{}{}:
			default:
			}
		default:
		}

		// done
		select {
		case <-doneChan:
			monrun = false
		default:
		}

		// get stats from expel
		select {
		case stats = <-o.statsChan:
			stats.Monitoring = monrun
		default:
		}

		// marshal
		data, err := json.Marshal(stats)
		if err != nil {
			fmt.Printf("%v\n", err)
		} else {
			// send stats to reservoird
			select {
			case statsChan <- string(data):
			default:
			}
		}

		if monrun == true {
			time.Sleep(time.Millisecond)
		}
	}
}

// Expel reads messages from a channel and writes them to stdout
func (o *Stdout) Expel(queues []icd.Queue, done <-chan struct{}, wg *sync.WaitGroup) error {
	defer wg.Done()

	stats := StdoutStats{}

	run := true
	stats.Running = run
	for run == true {
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

		// clear
		select {
		case <-o.clearChan:
			stats = StdoutStats{}
			stats.Running = run
		default:
		}

		// listen for shutdown
		select {
		case <-done:
			run = false
			stats.Running = run
		default:
		}

		// send to monitor
		select {
		case o.statsChan <- stats:
		default:
		}

		if run == true {
			time.Sleep(time.Millisecond)
		}
	}
	return nil
}
