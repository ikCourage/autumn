package bbq

import (
	"github.com/ikCourage/autumn/timer"
	"sync"
	"time"
)

const (
	broadcast_delay = time.Second
)

type team struct {
	party  *party
	id     string
	rwLK   sync.RWMutex
	head   *chum
	last   *chum
	msgLK  sync.Mutex
	msgMap map[int]message
	length uint32
}

type message struct {
	senders map[int]struct{}
}

func newTeam(party *party, id string) *team {
	return &team{
		party:  party,
		id:     id,
		msgMap: make(map[int]message),
	}
}

func (self *team) add(chum *chum) {
	self.rwLK.Lock()
	chum.team = self
	if nil == self.head {
		self.head = chum
	} else {
		chum.prev = self.last
		self.last.next = chum
	}
	self.last = chum
	self.length++
	self.rwLK.Unlock()
}

func (self *team) remove(chum *chum) {
	self.rwLK.Lock()
	if nil != chum.next {
		chum.next.prev = chum.prev
	}
	if nil != chum.prev {
		chum.prev.next = chum.next
	} else {
		self.head = chum.next
	}
	if chum == self.last {
		self.last = chum.next
	}
	if self.length == 1 {
		self.party.teamsLK.Lock()
		delete(self.party.teams, self.id)
		self.party.teamsLK.Unlock()
	} else {
		self.length--
	}
	self.rwLK.Unlock()
}

func (self *team) broadcast(chum *chum, b []byte, text bool) error {
	header := [4]byte{0x82, 0x7E}
	if text {
		header[0] = 0x81
	}
	var n int
	l := len(b)
	switch {
	case l < 0x7E:
		header[1] = byte(l)
		n = 2
	case l <= 0xFFFF:
		header[2] = byte(l >> 8)
		header[3] = byte(l & 0xFF)
		n = 4
	default:
		return Error_notsupport_length64
	}
	// 缓存起来，延迟发送（尽量合并相同的消息）
	hash := hash_times33(header[:], b)
	self.msgLK.Lock()
	msg, ok := self.msgMap[hash]
	if ok {
		msg.senders[chum.fd] = struct{}{}
		self.msgLK.Unlock()
		return nil
	}
	bs := pollbytes.Get(l+n, l+n)
	copy(bs, header[:])
	copy(bs[n:], b)
	msg = message{
		senders: make(map[int]struct{}),
	}
	msg.senders[chum.fd] = struct{}{}
	self.msgMap[hash] = msg
	self.msgLK.Unlock()

	timer.After(broadcast_delay, func() {
		self.rwLK.RLock()
		chum := self.head
		if nil != chum {
			self.msgLK.Lock()
			delete(self.msgMap, hash)
			self.msgLK.Unlock()
			for {
				chum.Write(bs)
				chum = chum.next
				if nil == chum {
					break
				}
			}
		}
		pollbytes.Put(bs)
		self.rwLK.RUnlock()
	})
	return nil
}

func hash_times33(b1, b2 []byte) int {
	var hash int = 5381
	for i, l := 0, len(b1); i < l; i++ {
		hash += (hash << 5) + int(b1[i])
	}
	for i, l := 0, len(b2); i < l; i++ {
		hash += (hash << 5) + int(b2[i])
	}
	return hash
}
