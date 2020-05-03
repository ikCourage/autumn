package pooll

import (
	"fmt"
	"runtime"
	"sync"
)

type task struct {
	next *task
	args interface{}
}

type poollTask struct {
	lk      sync.Mutex
	cond    *sync.Cond
	head    *task
	last    *task
	length  int
	max     int
	worker  int
	working int
	release bool
	handler func(v interface{})
}

type Pooll interface {
	Put(v interface{}) error
	Rel()
}

type Config struct {
	Max     int
	Lazy    bool
	Handler func(v interface{})
}

var (
	defaultConfig = &Config{}
)

func New(config *Config) Pooll {
	if nil == config {
		config = defaultConfig
	}
	max := config.Max
	if max <= 0 {
		max = runtime.GOMAXPROCS(0) << 1
	}
	self := &poollTask{
		max:     max,
		handler: config.Handler,
	}
	self.cond = sync.NewCond(&self.lk)
	if !config.Lazy {
		self.worker = max
		for max > 0 {
			max--
			go self.loop(max)
		}
	}
	return self
}

func (self *poollTask) Rel() {
	self.lk.Lock()
	self.release = true
	self.lk.Unlock()
	self.cond.Broadcast()
}

func (self *poollTask) Put(v interface{}) error {
	if nil == self.handler {
		if _, ok := v.(func()); ok == false {
			return fmt.Errorf("expect a function")
		}
	}
	self.lk.Lock()
	task := &task{
		args: v,
	}
	if nil == self.head {
		self.head = task
	} else {
		self.last.next = task
	}
	self.last = task
	self.length++
	if self.length < self.worker-self.working {
		self.lk.Unlock()
		self.cond.Signal()
	} else {
		if self.worker < self.max {
			self.worker++
			go self.loop(self.worker - 1)
		}
		self.lk.Unlock()
	}
	return nil
}

func (self *poollTask) loop(worker int) {
	b := true
__loop:
	self.cond.L.Lock()
__retry:
	if b {
		self.working++
	}
	task := self.head
	if nil == task {
		if self.release {
			self.cond.L.Unlock()
			return
		}
		self.working--
		self.cond.Wait()
		b = true
		goto __retry
	}
	if task == self.last {
		self.last = nil
	}
	self.head = task.next
	self.length--
	self.cond.L.Unlock()
	if nil != self.handler {
		self.handler(task.args)
	} else {
		task.args.(func())()
	}
	b = false
	goto __loop
}
