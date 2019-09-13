// +build darwin dragonfly freebsd netbsd openbsd

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
	fd, err := unix.Kqueue()
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
	_, err := unix.Kevent(self.fd, toChanges(uint64(fd), e, unix.EV_ADD), nil, nil)
	return err
}

func (self *poll) Mod(fd int, e KEvent) error {
	_, err := unix.Kevent(self.fd, toChanges(uint64(fd), e, unix.EV_ADD), nil, nil)
	return err
}

func (self *poll) Del(fd int) error {
	_, err := unix.Kevent(self.fd, toChanges(uint64(fd), 0, unix.EV_DELETE), nil, nil)
	return err
}

func (self *poll) loop() {
	const (
		min = 1 << 10
		max = 1 << 14
	)
	events := make([]unix.Kevent_t, min)
	kevs := make([]KEvent_t, min)

__loop:
	n, err := unix.Kevent(self.fd, nil, events, nil)
	if nil != err {
		if e, ok := err.(syscall.Errno); ok && e.Temporary() {
			goto __loop
		}
		return
	}
	for i := 0; i < n; i++ {
		kevs[i].Fd = int(events[i].Ident)
		kevs[i].Event = KEvent(toKEvent(events[i].Filter, events[i].Flags))
	}
	self.handler(kevs[:n])
	goto __loop
}

func toKEvent(filter int16, flags uint16) int {
	var e int
	if flags&unix.EV_EOF != 0 {
		e = KEV_HUP
	}
	if flags&unix.EV_ERROR != 0 {
		e |= KEV_ERR
	}
	switch filter {
	case unix.EVFILT_READ:
		e |= KEV_READ
		if flags&unix.EV_EOF != 0 {
			e = KEV_RDHUP
		}
	case unix.EVFILT_WRITE:
		e |= KEV_WRITE
	}
	return e
}

func toChanges(fd uint64, e KEvent, flags uint16) []unix.Kevent_t {
	var n int
	var changes [2]unix.Kevent_t

	if flags&unix.EV_ADD != 0 {
		if e&KEF_ONESHOT != 0 {
			flags |= unix.EV_ONESHOT
		}
		if e&KEF_ET != 0 {
			flags |= unix.EV_CLEAR
		}
	}

	if e&KEV_READ != 0 {
		changes[n].Ident = fd
		changes[n].Flags = flags
		changes[n].Filter = unix.EVFILT_READ
		n++
	}
	if e&KEV_WRITE != 0 {
		changes[n].Ident = fd
		changes[n].Flags = flags
		changes[n].Filter = unix.EVFILT_WRITE
		n++
	}

	return changes[:n]
}
