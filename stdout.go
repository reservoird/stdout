package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/reservoird/ibool"
)

// needed to aid in unit testing
type iwriter interface {
	WriteString(string) (int, error)
}

type boolbridge struct {
}

func (o *boolbridge) Val() bool {
	return true
}

type stdout struct {
	Timestamp   bool
	writer      iwriter
	keepRunning ibool.IBool
}

// Config configures consumer
func (o *stdout) Config(cfg string) error {
	// default
	o.Timestamp = false
	o.writer = bufio.NewWriter(os.Stdout)
	o.keepRunning = &boolbridge{}

	if cfg != "" {
		b, err := ioutil.ReadFile(cfg)
		if err != nil {
			return err
		}
		s := stdout{}
		err = json.Unmarshal(b, &s)
		if err != nil {
			return err
		}
		o.Timestamp = s.Timestamp
	}
	return nil
}

// Expel reads messages from a channel and writes them to stdout
func (o *stdout) Expel(channel <-chan []byte) error {
	for o.keepRunning.Val() == true {
		line := <-channel

		output := string(line)
		if o.Timestamp == true {
			output = fmt.Sprintf("rdetime %s: %s", time.Now().Format(time.RFC3339), line)
		}
		_, err := o.writer.WriteString(output)
		if err != nil {
			return err
		}
	}
	return nil
}

// Expeller for stdout
var Expeller stdout
