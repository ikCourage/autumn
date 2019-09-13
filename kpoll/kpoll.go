package kpoll

const (
	KEV_READ  = 0x1
	KEV_WRITE = 0x4
	KEV_ERR   = 0x8
	KEV_HUP   = 0x10
	KEV_RDHUP = 0x2000

	KEF_ONESHOT = 0x40000000
	KEF_ET      = 0x80000000
)

type KEvent int

type KEvent_t struct {
	Fd    int
	Event KEvent
}

type Kpoll interface {
	Add(fd int, e KEvent) error
	Mod(fd int, e KEvent) error
	Del(fd int) error
}
