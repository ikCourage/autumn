package bbq

import (
	"github.com/gobwas/ws"
	"github.com/ikCourage/autumn/kpoll"
	"github.com/ikCourage/autumn/pooll"
	"github.com/ikCourage/autumn/timer"
	"net"
	"sync"
	"time"
)

type party struct {
	upgrader *ws.Upgrader

	kpoller    kpoll.Kpoll
	poollWrite pooll.Pooll
	poollRead  pooll.Pooll

	chumsLk  sync.RWMutex
	chumsMap map[string]*chum
	chums    map[int]*chum
	head     *chum
	last     *chum
	cursor   *chum
	teamsLK  sync.RWMutex
	teams    map[string]*team
	routers  map[uint32]Router

	frequently      int64
	timeout         int64
	timeoutInterval int64
	clock           int64
	sleep           *time.Timer
	wakeup          chan struct{}
}

type Party interface {
	Listen(network, address string) error
	AddRouters(routers ...Router)
}

type Config struct {
	Upgrader        *ws.Upgrader
	Timeout         time.Duration
	TimeoutInterval time.Duration
}

var (
	defaultConfig = &Config{
		Timeout:         time.Minute * 4,
		TimeoutInterval: time.Minute,
	}
)

func New(config *Config) Party {
	if nil == config {
		config = defaultConfig
	}
	self := &party{
		upgrader:        config.Upgrader,
		timeout:         int64(config.Timeout),
		timeoutInterval: int64(config.TimeoutInterval),
	}
	if self.timeout <= 0 {
		self.timeout = int64(defaultConfig.Timeout)
	}
	if self.timeoutInterval <= 0 {
		self.timeoutInterval = int64(defaultConfig.TimeoutInterval)
	}
	return self
}

func (self *party) Listen(network, address string) (err error) {
	ln, err := net.Listen(network, address)
	if nil != err {
		return
	}

	if nil == self.upgrader {
		self.upgrader = &ws.Upgrader{
			ReadBufferSize:  512,
			WriteBufferSize: 256,
		}
	}

	poollAccept := pooll.New(&pooll.Config{
		Handler: func(v interface{}) {
			conn, err := ln.Accept()
			if nil != err {
				self.wakeup <- struct{}{}
				return
			}
			if nil != conn.SetReadDeadline(time.Now().Add(time.Millisecond*20)) {
				conn.Close()
				return
			}
			_, err = self.upgrader.Upgrade(conn)
			if nil != err {
				conn.Close()
				self.wakeup <- struct{}{}
				return
			}
			if nil != conn.SetReadDeadline(time.Time{}) {
				conn.Close()
				return
			}

			chum := &chum{
				Conn:   conn,
				fd:     kpoll.Sysfd_unsafe(conn),
				party:  self,
				active: timer.Now(),
			}
			self.chumsLk.Lock()
			self.chums[chum.fd] = chum
			if nil == self.head {
				self.head = chum
			} else {
				chum.aprev = self.last
				self.last.anext = chum
			}
			self.last = chum
			self.chumsLk.Unlock()
			self.kpoller.Add(chum.fd, kpoll.KEV_READ|kpoll.KEF_ET)
		},
	})

	self.chumsMap = make(map[string]*chum)
	self.chums = make(map[int]*chum)
	self.teams = make(map[string]*team)
	self.wakeup = make(chan struct{}, 1)

	self.poollRead = pooll.New(&pooll.Config{
		Handler: readHandler,
	})
	self.poollWrite = pooll.New(&pooll.Config{
		Handler: writeHandler,
	})

	skFd := kpoll.Sysfd_unsafe(ln)
	self.kpoller, err = kpoll.New(func(events []kpoll.KEvent_t) {
		lk := 0
		for i, l := 0, len(events); i < l; i++ {
			if events[i].Fd == skFd {
				poollAccept.Put(nil)
			} else {
				if lk == 0 {
					lk = 1
					self.chumsLk.RLock()
				}
				chum := self.chums[events[i].Fd]
				if nil == chum {
					continue
				}
				switch {
				case events[i].Event&(kpoll.KEV_HUP|kpoll.KEV_RDHUP|kpoll.KEV_ERR) != 0:
					if lk == 1 {
						lk = 0
						self.chumsLk.RUnlock()
					}
					chum.Close()
				case events[i].Event&kpoll.KEV_READ != 0:
					self.poollRead.Put(chum)
				case events[i].Event&kpoll.KEV_WRITE != 0:
					self.poollWrite.Put(chum)
				}
			}
		}
		if lk == 1 {
			self.chumsLk.RUnlock()
		}
	})
	if nil != err {
		return
	}
	self.kpoller.Add(skFd, kpoll.KEV_READ|kpoll.KEF_ET)

	go self.timeoutLoop()
	return
}

func (self *party) AddRouters(routers ...Router) {
	if nil == self.routers {
		self.routers = make(map[uint32]Router, len(routers))
	}
	for _, v := range routers {
		self.routers[v.Action] = v
	}
}

func (self *party) timeoutLoop() {
__loop:
	closed, count := 0, 0
	self.chumsLk.Lock()
	if nil == self.cursor {
		self.cursor = self.head
	}
	chum := self.cursor
	now := timer.Now()
	// 防止过于频繁
	if now > self.clock+int64(time.Millisecond*100) {
		// closed 防止一次性关闭过多，导致大量瞬时重连
		// count 防止一次性遍历过多
		for nil != self.cursor && closed < 1000 && count < 100000 {
			count++
			if now >= self.cursor.active+self.timeout {
				self.cursor.close(true)
				closed++
			} else {
				self.cursor = self.cursor.anext
			}
			if nil == self.cursor {
				self.cursor = self.head
			}
			if chum == self.cursor {
				// 跳出首尾环
				break
			}
		}
	}
	self.chumsLk.Unlock()
	now = timer.Now()
	if now < self.clock+int64(time.Second) {
		self.frequently++
		if self.frequently > 4 {
			// TODO: 可能会频繁触发检测，需要观察原因（应该是由 accept 引起的）
		}
	} else {
		self.frequently = 0
	}
	self.clock = now
	if nil == self.sleep {
		self.sleep = time.NewTimer(time.Duration(self.timeoutInterval))
	} else {
		self.sleep.Reset(time.Duration(self.timeoutInterval))
	}
	if nil != self.sleep {
		select {
		case <-self.sleep.C:
			self.sleep = nil
		case <-self.wakeup:
			if nil != self.sleep {
				if !self.sleep.Stop() {
					self.sleep = nil
				}
			}
		}
	} else {
		<-self.wakeup
	}
	goto __loop
}

func writeHandler(v interface{}) {
	v.(*chum).writeLoop()
}

func readHandler(v interface{}) {
	v.(*chum).readLoop()
}
