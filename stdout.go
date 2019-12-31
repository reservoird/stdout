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
	stats     StdoutStats
	statsLock sync.Mutex
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
		stats:     StdoutStats{},
		statsLock: sync.Mutex{},
	}
	return o, nil
}

// Name provides name of expeller
func (o *Stdout) Name() string {
	return o.cfg.Name
}

// Stats return stats NOTE: thread safe
func (o *Stdout) Stats() (string, error) {
	o.statsLock.Lock()
	defer o.statsLock.Unlock()
	data, err := json.Marshal(o.stats)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ClearStats clears stats NOTE: thread safe
func (o *Stdout) ClearStats() {
	o.statsLock.Lock()
	defer o.statsLock.Unlock()
	o.stats = StdoutStats{}
}

func (o *Stdout) incMessagesReceived() {
	o.statsLock.Lock()
	defer o.statsLock.Unlock()
	o.stats.MessagesReceived = o.stats.MessagesReceived + 1
}

func (o *Stdout) incMessagesSent() {
	o.statsLock.Lock()
	defer o.statsLock.Unlock()
	o.stats.MessagesSent = o.stats.MessagesSent + 1
}

func (o *Stdout) setRunning(run bool) {
	o.statsLock.Lock()
	defer o.statsLock.Unlock()
	o.stats.Running = run
}

// Expel reads messages from a channel and writes them to stdout
func (o *Stdout) Expel(queues []icd.Queue, done <-chan struct{}, wg *sync.WaitGroup) error {
	defer wg.Done()

	o.run = true
	o.setRunning(o.run)
	for o.run == true {
		for q := range queues {
			if queues[q].Closed() == false {
				d, err := queues[q].Get()
				if err != nil {
					fmt.Printf("%v\n", err)
				} else {
					if d != nil {
						o.incMessagesReceived()
						data, ok := d.([]byte)
						if ok == false {
							fmt.Printf("error invalid type\n")
						} else {
							line := string(data)
							if o.cfg.Timestamp == true {
								line = fmt.Sprintf("[%s %s] ", o.Name(), time.Now().Format(time.RFC3339)) + line
							}
							fmt.Printf("%s", line)
							o.incMessagesSent()
						}
					}
				}
			}
		}

		select {
		case <-done:
			o.run = false
			o.setRunning(o.run)
		default:
		}

		time.Sleep(time.Millisecond)
	}
	return nil
}
