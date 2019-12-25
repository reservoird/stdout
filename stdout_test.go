package main

import (
	"bytes"
	"testing"
)

type boolbridgetest struct {
	count int
	val   bool
}

func newboolbridgetest() *boolbridgetest {
	b := new(boolbridgetest)
	b.count = 0
	b.val = true
	return b
}

func (o *boolbridgetest) Val() bool {
	val := o.val
	if o.count == 1 {
		o.val = false
	}
	o.count = o.count + 1
	return val
}

func TestConfig(t *testing.T) {
	s := stdout{}
	err := s.Config("")
	if err != nil {
		t.Errorf("expecting nil but got error: %v", err)
	}
	if s.keepRunning.Val() == false {
		t.Errorf("expecting true but got false")
	}
}

func TestExpel(t *testing.T) {
	s := stdout{}
	s.keepRunning = newboolbridgetest()
	s.writer = bytes.NewBufferString("")
	src := make(chan []byte, 2)
	expected := []byte("hello")
	src <- expected
	err := s.Expel(src)
	if err != nil {
		t.Errorf("expecting nil but got error: %v", err)
	}
	_, ok := s.writer.(*bytes.Buffer)
	if ok == false {
		t.Errorf("invalid writer byte")
	}
	actual := s.writer.(*bytes.Buffer).String()
	if string(actual) != string(expected) {
		t.Errorf("expecting %s but got %s", string(expected), string(actual))
	}
}
