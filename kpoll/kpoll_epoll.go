// +build linux

package kpoll

import (
	"golang.org/x/sys/unix"
	"syscall"
)

type poll struct {
	fd      int
	handler func([]KEvent_t)
}

func New(handler func([]KEvent_t)) (Kpoll, error) {
	fd, err := unix.EpollCreate1(0)
	if nil != err {
		return nil, err
	}
	self := &poll{
		fd:      fd,
		handler: handler,
	}
	go self.loop()
	return self, nil
}

func (self *poll) Add(fd int, e KEvent) error {
	if err := syscall.SetNonblock(fd, true); nil != err {
		return err
	}
	return unix.EpollCtl(self.fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Fd:     int32(fd),
		Events: uint32(e),
	})
}

func (self *poll) Mod(fd int, e KEvent) error {
	return unix.EpollCtl(self.fd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{
		Fd:     int32(fd),
		Events: uint32(e),
	})
}

func (self *poll) Del(fd int) error {
	return unix.EpollCtl(self.fd, unix.EPOLL_CTL_DEL, fd, nil)
}

func (self *poll) loop() {
	const (
		min = 1 << 10
		max = 1 << 14
	)
	events := make([]unix.EpollEvent, min)
	kevs := make([]KEvent_t, min)

__loop:
	n, err := unix.EpollWait(self.fd, events, -1)
	if nil != err {
		if e, ok := err.(syscall.Errno); ok && e.Temporary() {
			goto __loop
		}
		return
	}
	for i := 0; i < n; i++ {
		kevs[i].Fd = int(events[i].Fd)
		kevs[i].Event = KEvent(events[i].Events)
	}
	self.handler(kevs[:n])
	goto __loop
}
