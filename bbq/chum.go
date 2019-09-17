package bbq

import (
	"fmt"
	"github.com/gobwas/pool/pbytes"
	"github.com/ikCourage/autumn/kpoll"
	"github.com/ikCourage/autumn/timer"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	header_fin  = 0x80
	header_mask = 0x40
	header_read = 0x20

	flag_action  = 0x1
	flag_length  = 0x2
	flag_data    = 0x4
	flag_all     = 0x8
	flag_stream  = 0x10
	flag_again   = 0x20
	flag_discard = 0x80
)

var (
	Error_notsupport_rsv      = fmt.Errorf("not support rsv")
	Error_notsupport_length64 = fmt.Errorf("not support 64-bit length")
	Error_opcode              = fmt.Errorf("error opcode")
	Error_closed              = fmt.Errorf("the connection is closed")

	Error_action_notfound = fmt.Errorf("action not found")
	Error_action          = fmt.Errorf("action must be less than 32 bits")
	Error_length          = fmt.Errorf("length must be less than 16 bits")
	Error_notenough       = fmt.Errorf("the length is not enough")
)

var (
	frame_ping = []byte{0x89, 0}
	frame_pong = []byte{0x8A, 0}
)

var (
	PBytes = pbytes.New(2, 65536)
)

const (
	ActionType_discard = 0
	ActionType_vlen    = -1
	ActionType_stream  = -2
	ActionType_all     = -3
)

type Router struct {
	Type    int32
	Action  uint32
	Handler func(chum Chum)
}

type chum struct {
	net.Conn
	prev          *chum
	next          *chum
	aprev         *chum
	anext         *chum
	party         *party
	team          *team
	handler       func(chum Chum)
	mask          [8]byte // 也用于 header 的缓冲
	payloadOffset uint32  // 当前已读的偏移量
	payloadLength uint32  // 有效数据长度
	offset        uint32  // 应用数据偏移量
	length        uint32  // 应用数据长度
	action        uint32
	flags         uint8 // 标志 action length 等是否准备好
	header        uint8
	opCode        uint8
	_             uint8
	readBuf       []byte
	writeBuf      []byte
	writeLK       sync.Mutex
	writeOffset   uint32
	reading       uint32
	closed        uint32
	active        int64
	fd            int
}

type Chum interface {
	Close() error
	Read(b []byte) (int, error)
	Write(b []byte) (int, error)

	Join(id string)
	Broadcast(b []byte, text bool, delay time.Duration) error
	WriteFrame(b []byte, text bool) (int, error)

	Closed() bool
	First() bool
	Received() bool

	Action() uint32
	Length() uint32
	Data() []byte
}

func (self *chum) close(locked bool) (err error) {
	if !atomic.CompareAndSwapUint32(&self.closed, 0, 1) {
		return Error_closed
	}
	if !locked {
		self.party.chumsLk.Lock()
	}
	delete(self.party.chums, self.fd)
	if nil != self.anext {
		self.anext.aprev = self.aprev
	}
	if nil != self.aprev {
		self.aprev.anext = self.anext
	} else {
		self.party.head = self.anext
	}
	if self == self.party.last {
		self.party.last = self.aprev
	}
	if self == self.party.cursor {
		self.party.cursor = self.anext
	}
	self.party.kpoller.Del(self.fd)
	if !locked {
		self.party.chumsLk.Unlock()
	}
	if nil != self.team {
		self.team.remove(self)
		self.team = nil
	}
	if nil != self.readBuf {
		PBytes.Put(self.readBuf)
		self.readBuf = nil
	}
	if nil != self.writeBuf {
		PBytes.Put(self.writeBuf)
		self.writeBuf = nil
	}
	err = self.Conn.Close()
	self.prev = nil
	self.next = nil
	self.aprev = nil
	self.anext = nil
	self.party = nil
	if nil != self.handler && self.flags&flag_stream != 0 {
		self.handler(self)
		self.handler = nil
	}
	self.flags = 0
	return
}

func (self *chum) Close() error {
	return self.close(false)
}

func (self *chum) Closed() bool {
	return atomic.LoadUint32(&self.closed) == 1
}

func (self *chum) Join(id string) {
	if nil != self.team {
		self.team.remove(self)
	}
	self.party.teamsLK.Lock()
	team, ok := self.party.teams[id]
	if !ok {
		team = newTeam(self.party, id)
		self.party.teams[id] = team
	}
	team.add(self)
	self.party.teamsLK.Unlock()
}

func (self *chum) Broadcast(b []byte, text bool, delay time.Duration) error {
	if nil != self.team {
		return self.team.broadcast(self, b, text, delay)
	}
	return nil
}

func (self *chum) First() bool {
	return self.flags&flag_again == 0
}

func (self *chum) Received() bool {
	if self.header&header_fin != 0 {
		if self.flags&flag_stream != 0 {
			return self.payloadOffset == self.payloadLength
		}
		return self.offset == self.length
	}
	return false
}

func (self *chum) Action() uint32 {
	return self.action
}

func (self *chum) Length() uint32 {
	return self.length
}

func (self *chum) Data() []byte {
	return self.readBuf
}

func (self *chum) Read(b []byte) (int, error) {
	n, err := syscall.Read(self.fd, b)
	if nil != err {
		n = 0
		switch err {
		case syscall.EAGAIN, syscall.EINTR:
		default:
			self.Close()
		}
	} else if n <= 0 {
		err = syscall.EAGAIN
	}
	return n, err
}

func (self *chum) write(b []byte) (int, error) {
	n, err := syscall.Write(self.fd, b)
	if nil != err {
		n = 0
		switch err {
		case syscall.EAGAIN, syscall.EINTR:
			if nil == self.writeBuf {
				// #ctr 开始侦听 write
				self.party.kpoller.Mod(self.fd, kpoll.KEV_WRITE|kpoll.KEF_ET)
			}
		default:
			self.Close()
		}
	} else if n <= 0 {
		err = syscall.EAGAIN
	}
	return n, err
}

func (self *chum) Write(b []byte) (nn int, err error) {
	var n int
	nn = len(b)
	self.writeLK.Lock()
	defer self.writeLK.Unlock()
	if nil == self.writeBuf {
		// 尝试写一次
		n, err = self.write(b)
		if nil != err {
			switch err {
			case syscall.EAGAIN, syscall.EINTR:
			default:
				return n, err
			}
		}
		if n < nn {
			n = nn - n
			self.writeOffset = uint32(n)
			self.writeBuf = PBytes.Get(0, n)
			self.writeBuf = self.writeBuf[:cap(self.writeBuf)]
			copy(self.writeBuf, b)

			if nil == err {
				// 加入写线程
				self.party.poollWrite.Put(self)
			}
		}
	} else {
		n = cap(self.writeBuf)
		if nn > n-int(self.writeOffset) {
			buf := self.writeBuf
			self.writeBuf = PBytes.Get(0, int(self.writeOffset)+nn)
			self.writeBuf = self.writeBuf[:cap(self.writeBuf)]
			copy(self.writeBuf, buf[:self.writeOffset])
			PBytes.Put(buf)
		}
		copy(self.writeBuf[self.writeOffset:], b)
		self.writeOffset += uint32(nn)
	}
	return
}

func (self *chum) WriteFrame(b []byte, text bool) (int, error) {
	header := [4]byte{0x82, 0x7E}
	if text {
		header[0] = 0x81
	}
	l := len(b)
	switch {
	case l < 0x7E:
		header[1] = byte(l)
		l = 2
	case l <= 0xFFFF:
		header[2] = byte(l >> 8)
		header[3] = byte(l & 0xFF)
		l = 4
	default:
		return 0, Error_notsupport_length64
	}
	self.Write(header[:l])
	return self.Write(b)
}

func (self *chum) writeLoop() error {
	self.writeLK.Lock()
	defer self.writeLK.Unlock()
	n, err := self.write(self.writeBuf[:self.writeOffset])
	if nil != err {
		return err
	}
	if n != int(self.writeOffset) {
		copy(self.writeBuf, self.writeBuf[n:self.writeOffset])
		self.writeOffset -= uint32(n)
	} else {
		PBytes.Put(self.writeBuf)
		self.writeBuf = nil
		// #ctr 重新侦听 read
		self.party.kpoller.Mod(self.fd, kpoll.KEV_READ|kpoll.KEF_ET)
	}
	return err
}

func (self *chum) readHeader() (err error) {
	// if self.header&header_read != 0 {
	// 	return
	// }
	var n int
	bs := self.mask[:]
__retry:
	if self.payloadOffset < 2 {
		// 读取头两个字节
		n, err = self.Read(bs[self.payloadOffset:2])
		if nil != err {
			return
		}
		self.payloadOffset += uint32(n)
		if self.payloadOffset == 2 {
			if bs[0]&0x70 != 0 {
				// 不支持 rsv
				self.Close()
				return Error_notsupport_rsv
			}
			n = 2
			self.header = bs[0] & header_fin
			self.opCode = bs[0] & 0x0F
			if bs[1]&0x80 != 0 {
				self.header |= header_mask
				n += 4
			}
			self.payloadLength = uint32(bs[1] & 0x7F)
			switch {
			case self.payloadLength < 126:
			case self.payloadLength == 126:
				n += 2
			default:
				// 不支持长数据
				self.Close()
				return Error_notsupport_length64
			}
			switch self.opCode {
			case 0x0, 0x1, 0x2, 0x9, 0xA:
			case 0x8:
				self.Close()
				return Error_closed
			default:
				self.Close()
				return Error_opcode
			}
			bs[0] = uint8(n)
		} else {
			return
		}
	} else {
		if self.payloadOffset == 2 {
			// 如果同时有多个线程在读的话，那么第二个线程将经过此处
			// 事实上，此时该连接已经由第一个线程关闭了
			return Error_closed
		}
		n = int(bs[0])
	}
	if self.payloadOffset < uint32(n) {
		n, err = self.Read(bs[self.payloadOffset:n])
		if nil != err {
			return
		}
		self.payloadOffset += uint32(n)
		if self.payloadOffset == uint32(bs[0]) {
			// extLen + mask
			if self.payloadLength == 126 {
				self.payloadLength = (uint32(bs[2]) << 8) | uint32(bs[3])
			}
			if self.header&header_mask != 0 {
				copy(bs[:4], bs[self.payloadOffset-4:])
			}
		} else {
			return
		}
	}
	self.header |= header_read
	self.payloadOffset = 0

	switch self.opCode {
	case 0x9:
		// ping，返回 pong
		self.active = timer.Now()
		self.flags |= flag_discard
		self.Write(frame_pong)
	case 0xA:
		// pong，丢弃后续数据
		self.active = timer.Now()
		self.flags |= flag_discard
	}
	if self.payloadLength == 0 {
		// 没有数据，当前帧结束
		self.header = 0
		goto __retry
	}
	if self.flags&flag_all != 0 {
		// 跨帧数据
		self.length += self.payloadLength
		if self.offset == 0 {
			// 待读取的缓冲
			self.readBuf = PBytes.Get(int(self.length), int(self.length))
		} else {
			bs = self.readBuf
			self.readBuf = PBytes.Get(int(self.length), int(self.length))
			copy(self.readBuf[self.offset:], bs)
			PBytes.Put(bs)
		}
	}
	return
}

func (self *chum) readAction() (err error) {
	// if self.flags&flag_action != 0 {
	// 	return
	// }
	bs := self.mask[4:5]
__retry:
	_, err = self.Read(bs)
	if nil != err {
		return
	}
	bs[0] ^= self.mask[self.payloadOffset&3]
	self.payloadOffset++
	self.action |= uint32(bs[0]&0x7F) << uint32(self.offset*7)
	if bs[0]&0x80 != 0 {
		// action 为多字节
		if self.offset == 4 {
			self.Close()
			return Error_action
		}
		self.offset++
		goto __retry
	} else {
		self.offset = 0
		self.flags |= flag_action

		// 根据 action type 确定是否读取后续数据
		if router, ok := self.party.routers[self.action]; ok {
			self.handler = router.Handler
			switch router.Type {
			case ActionType_vlen:
			case ActionType_all:
				self.flags |= flag_length | flag_all
				self.length = self.payloadLength - self.payloadOffset
				if self.length != 0 {
					// 待读取的缓冲
					self.readBuf = PBytes.Get(int(self.length), int(self.length))
				}
			case ActionType_stream:
				self.flags |= flag_length | flag_stream
				// 待读取的缓冲
				self.readBuf = PBytes.Get(1024, 1024)
			default:
				self.flags |= flag_data | flag_discard

				// 更新活跃时间（有效的通信才是活跃的）
				self.active = timer.Now()
				self.handler(self)
			}
		} else {
			self.Close()
			return Error_action_notfound
		}
	}
	return
}

func (self *chum) readLength() (err error) {
	// if self.flags&flag_length != 0 {
	// 	return
	// }
	bs := self.mask[4:5]
__retry:
	_, err = self.Read(bs)
	if nil != err {
		return
	}
	bs[0] ^= self.mask[self.payloadOffset&3]
	self.payloadOffset++
	self.length |= uint32(bs[0]&0x7F) << uint32(self.offset*7)
	if bs[0]&0x80 != 0 {
		// length 为多字节
		if self.offset == 2 {
			self.Close()
			return Error_length
		}
		self.offset++
		goto __retry
	} else {
		if self.length > self.payloadLength-self.payloadOffset && self.header&header_fin != 0 {
			self.Close()
			return Error_notenough
		}
		self.offset = 0
		self.flags |= flag_length
		if self.length != 0 {
			// 待读取的缓冲
			self.readBuf = PBytes.Get(int(self.length), int(self.length))
		}
	}
	return
}

func (self *chum) readData() (err error) {
	// if self.flags&flag_data != 0 {
	// 	return
	// }
	if self.flags&flag_length == 0 {
		if nil != self.readLength() {
			return
		}
	}
	var n int
	n, err = self.Read(self.readBuf[self.offset:])
	if nil != err {
		return
	}
	for n > 0 {
		n--
		self.readBuf[self.offset] ^= self.mask[self.payloadOffset&3]
		self.offset++
		self.payloadOffset++
	}
	if self.flags&flag_stream != 0 {
		// 更新活跃时间（有效的通信才是活跃的）
		self.active = timer.Now()
		self.handler(self)
		// 因为是渐进流，所以重置偏移，以待下次缓冲
		self.offset = 0
		self.flags |= flag_again
	} else if self.offset == self.length && self.header&header_fin != 0 {
		self.offset = 0
		self.flags |= flag_data | flag_discard

		// 更新活跃时间（有效的通信才是活跃的）
		self.active = timer.Now()
		self.handler(self)
	}
	return
}

func (self *chum) readLoop() {
	// 同一时刻，只有一个线程可以读
	if !atomic.CompareAndSwapUint32(&self.reading, 0, 1) {
		if party := self.party; nil != party {
			party.poollRead.Put(self)
		}
		return
	}

	if self.header&header_read == 0 {
		if nil != self.readHeader() {
			goto __end
		}
	}
	if self.flags&flag_action == 0 {
		if nil != self.readAction() {
			goto __end
		}
	}
	if self.flags&flag_action != 0 {
		if self.flags&flag_data == 0 {
			if nil != self.readData() {
				goto __end
			}
		}
	}

	if self.flags&flag_discard != 0 {
		// 无效数据，需要丢弃
		if self.payloadOffset < self.payloadLength {
			n, _ := io.CopyN(ioutil.Discard, self, int64(self.payloadLength-self.payloadOffset))
			self.payloadOffset += uint32(n)
			goto __end
		}
	}

	if self.payloadOffset == self.payloadLength {
		// 读取完毕，重置 header
		if self.header&header_fin != 0 {
			self.flags = 0
			self.offset = 0
			self.length = 0
			self.action = 0
			self.handler = nil
			if nil != self.readBuf {
				PBytes.Put(self.readBuf)
				self.readBuf = nil
			}
		}
		self.header = 0
		self.payloadOffset = 0
		self.payloadLength = 0
		atomic.StoreUint32(&self.reading, 0)
		if party := self.party; nil != party {
			party.poollRead.Put(self)
		}
		return
	}

__end:
	atomic.StoreUint32(&self.reading, 0)
}
