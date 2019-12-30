package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/reservoird/icd"
)

type stdoutCfg struct {
	Name      string
	Timestamp bool
}

type stdoutStats struct {
	MsgsRead uint64
	MsgsSent uint64
	Active   bool
}

type stdout struct {
	cfg       stdoutCfg
	stats     stdoutStats
	statsChan chan<- string
}

// New is what reservoird to create and start stdout
func New(cfg string, statsChan chan<- string) (icd.Expeller, error) {
	c := stdoutCfg{
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
	o := &stdout{
		cfg:       c,
		stats:     stdoutStats{},
		statsChan: statsChan,
	}
	return o, nil
}

// Name provides name of expeller
func (o *stdout) Name() string {
	return o.cfg.Name
}

// Expel reads messages from a channel and writes them to stdout
func (o *stdout) Expel(queues []icd.Queue, done <-chan struct{}, wg *sync.WaitGroup) error {
	defer wg.Done()

	o.stats.Active = true
	for o.stats.Active == true {
		for q := range queues {
			if queues[q].Closed() == false {
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
							if o.cfg.Timestamp == true {
								line = fmt.Sprintf("[%s %s] ", o.Name(), time.Now().Format(time.RFC3339)) + line
							}
							fmt.Printf("%s", line)
						}
					}
				}
			}
		}

		select {
		case <-done:
			o.stats.Active = false
		default:
			time.Sleep(time.Millisecond)
		}

		stats, err := json.Marshal(o.stats)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
		select {
		case o.statsChan <- string(stats):
		default:
		}
	}
	return nil
}
