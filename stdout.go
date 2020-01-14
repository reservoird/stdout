package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	Name             string
	MessagesReceived uint64
	MessagesSent     uint64
	Running          bool
}

// Stdout contains what is needed for expeller
type Stdout struct {
	cfg StdoutCfg
	run bool
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
		cfg: c,
		run: false,
	}
	return o, nil
}

// Name provides name of expeller
func (o *Stdout) Name() string {
	return o.cfg.Name
}

// Running states whether or not expel is running
func (o *Stdout) Running() bool {
	return o.run
}

// Expel reads messages from a channel and writes them to stdout
func (o *Stdout) Expel(queues []icd.Queue, mc *icd.MonitorControl) {
	defer mc.WaitGroup.Done()

	stats := StdoutStats{}

	o.run = true
	stats.Name = o.cfg.Name
	stats.Running = o.run
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

		// clear
		select {
		case <-mc.ClearChan:
			stats = StdoutStats{}
			stats.Name = o.cfg.Name
			stats.Running = o.run
		default:
		}

		// send to monitor
		select {
		case mc.StatsChan <- stats:
		default:
		}

		// listen for shutdown
		select {
		case <-mc.DoneChan:
			o.run = false
			stats.Running = o.run
		default:
		}

		if o.run == true {
			time.Sleep(time.Millisecond)
		}
	}

	// send final stats
	select {
	case mc.StatsChan <- stats:
	case <-time.After(time.Second):
	}
}
