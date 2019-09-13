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
	length  uint32
	worker  uint32
	max     uint32
	working uint32
	release bool
	handler func(v interface{})
}

type Pooll interface {
	Put(v interface{}) error
	Rel()
}

type Config struct {
	Handler func(v interface{})
}

var (
	defaultConfig = &Config{}
)

func New(config *Config) Pooll {
	if nil == config {
		config = defaultConfig
	}
	self := &poollTask{
		handler: config.Handler,
	}
	self.cond = sync.NewCond(&self.lk)
	return self
}

func (self *poollTask) Rel() {
	self.lk.Lock()
	self.head = nil
	self.last = nil
	self.handler = nil
	self.release = true
	self.lk.Unlock()
	self.cond.Broadcast()
}

func (self *poollTask) Put(v interface{}) error {
	if nil == self.handler {
		if _, ok := v.(func()); ok == false {
			return fmt.Errorf("not func")
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
	if worker := self.worker; worker > self.working {
		self.lk.Unlock()
		self.cond.Signal()
	} else {
		max := self.max
		if max == 0 {
			max = uint32(runtime.GOMAXPROCS(0))
		}
		if worker < max {
			self.worker++
			go self.loop(worker)
		}
		self.lk.Unlock()
	}
	return nil
}

func (self *poollTask) loop(worker uint32) {
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
			return
		}
		b = true
		self.working--
		self.cond.Wait()
		goto __retry
	}
	if task == self.last {
		self.last = nil
	}
	self.head = task.next
	self.cond.L.Unlock()
	b = false
	if nil != self.handler {
		self.handler(task.args)
	} else {
		task.args.(func())()
	}
	goto __loop
}
