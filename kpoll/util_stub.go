// +build !linux,!darwin,!dragonfly,!freebsd,!netbsd,!openbsd

package kpoll

import (
	"fmt"
)

func Sysfd_unsafe(v interface{}) int {
	panic(fmt.Errorf("Sysfd_unsafe is not supported on this operating system"))
	return 0
}
