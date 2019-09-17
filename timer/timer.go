package timer

import (
	"github.com/ikCourage/autumn/pooll"
	"sync"
	"time"
	_ "unsafe"
)

type timer struct {
	when int64
	f    func()
}

type timers struct {
	lk     sync.Mutex
	queue  []timer
	left   int
	right  int
	sleep  *time.Timer
	wakeup chan struct{}
	pooll  pooll.Pooll
}

var (
	defaultTimers timers
)

func init() {
	defaultTimers = timers{
		queue: make([]timer, 64),
		pooll: pooll.New(&pooll.Config{
			Handler: runHandler,
		}),
	}
}

//go:noescape
//go:linkname Now runtime.nanotime
func Now() int64

func After(d time.Duration, f func()) {
	if d <= 0 {
		d = time.Millisecond * 4
	}
	when := Now() + int64(d)
	defaultTimers.lk.Lock()
	if defaultTimers.left == defaultTimers.right {
		defaultTimers.left = cap(defaultTimers.queue) / 3
		defaultTimers.right = defaultTimers.left + 1
		defaultTimers.queue[defaultTimers.left].when = when
		defaultTimers.queue[defaultTimers.left].f = f
		if nil == defaultTimers.wakeup {
			defaultTimers.wakeup = make(chan struct{}, 1)
			go loop()
		} else {
			// 立刻唤醒
			defaultTimers.wakeup <- struct{}{}
		}
	} else {
		if when >= defaultTimers.queue[defaultTimers.right-1].when {
			c := cap(defaultTimers.queue)
			if defaultTimers.right == c {
				if defaultTimers.left <= c>>3 {
					// 空间过小，需要扩容
					realloc(c << 1)
				} else {
					// 左移
					move(defaultTimers.left >> 1)
				}
			}
			defaultTimers.queue[defaultTimers.right].when = when
			defaultTimers.queue[defaultTimers.right].f = f
			defaultTimers.right++
		} else if when < defaultTimers.queue[defaultTimers.left].when {
			if defaultTimers.left == 0 {
				c := cap(defaultTimers.queue)
				if c-defaultTimers.right <= c>>3 {
					// 空间过小，需要扩容
					realloc(c << 1)
				} else {
					// 右移
					move((c - defaultTimers.right) >> 1)
				}
			}
			defaultTimers.left--
			defaultTimers.queue[defaultTimers.left].when = when
			defaultTimers.queue[defaultTimers.left].f = f
			// 立刻唤醒
			defaultTimers.wakeup <- struct{}{}
		} else {
			c := cap(defaultTimers.queue)
			if defaultTimers.right == c {
				if defaultTimers.left <= c>>3 {
					// 空间过小，需要扩容
					realloc(c << 1)
				} else {
					// 左移
					move(defaultTimers.left >> 1)
				}
			}
			// search
			left, right := defaultTimers.left, defaultTimers.right-1
			for left < right {
				middle := (left + right) >> 1
				if when < defaultTimers.queue[middle].when {
					right = middle - 1
				} else {
					left = middle + 1
				}
			}
			if when == defaultTimers.queue[right].when {
				right++
			}
			copy(defaultTimers.queue[right+1:], defaultTimers.queue[right:defaultTimers.right])
			defaultTimers.queue[right].when = when
			defaultTimers.queue[right].f = f
			defaultTimers.right++
		}
	}
	defaultTimers.lk.Unlock()
}

func loop() {
__loop:
	defaultTimers.lk.Lock()
	now := Now()
	for defaultTimers.left < defaultTimers.right {
		if now >= defaultTimers.queue[defaultTimers.left].when {
			defaultTimers.pooll.Put(defaultTimers.queue[defaultTimers.left].f)
			defaultTimers.left++
		} else {
			break
		}
	}
	c := cap(defaultTimers.queue)
	if c > 1024 {
		if defaultTimers.right-defaultTimers.left < c>>2 {
			c = ceilToPowerOfTwo((defaultTimers.right - defaultTimers.left) * 3)
			if c < 64 {
				c = 64
			}
			realloc(c)
		}
	}
	if defaultTimers.left < defaultTimers.right {
		now = defaultTimers.queue[defaultTimers.left].when - Now()
		if nil == defaultTimers.sleep {
			defaultTimers.sleep = time.NewTimer(time.Duration(now))
		} else {
			defaultTimers.sleep.Reset(time.Duration(now))
		}
	}
	defaultTimers.lk.Unlock()
	if nil != defaultTimers.sleep {
		select {
		case <-defaultTimers.sleep.C:
			defaultTimers.sleep = nil
		case <-defaultTimers.wakeup:
			if nil != defaultTimers.sleep {
				if !defaultTimers.sleep.Stop() {
					defaultTimers.sleep = nil
				}
			}
		}
	} else {
		<-defaultTimers.wakeup
	}
	goto __loop
}

func runHandler(v interface{}) {
	v.(func())()
}

func realloc(c int) {
	arr := make([]timer, c)
	left := c / 3
	copy(arr[left:], defaultTimers.queue[defaultTimers.left:defaultTimers.right])
	defaultTimers.queue = arr
	defaultTimers.right += left - defaultTimers.left
	defaultTimers.left = left
}

func move(left int) {
	copy(defaultTimers.queue[left:], defaultTimers.queue[defaultTimers.left:defaultTimers.right])
	defaultTimers.right += left - defaultTimers.left
	defaultTimers.left = left
}

func ceilToPowerOfTwo(n int) int {
	if n <= 2 {
		return n
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}
