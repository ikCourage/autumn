// +build linux darwin dragonfly freebsd netbsd openbsd

package kpoll

import (
	"unsafe"
)

func Sysfd_unsafe(v interface{}) int {
	p := ((*[2]unsafe.Pointer)(unsafe.Pointer(&v))[1])

	p = *(*unsafe.Pointer)(p)
	return *(*int)(unsafe.Pointer(uintptr(p) + 16))
}
