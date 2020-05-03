package qsarray

const (
	min = 64
	max = 1024
)

type Value interface {
	Compare(Value) int
}

type QSArray struct {
	arr      []Value
	min      int
	capacity int
	left     int
	right    int
}

func (self *QSArray) Init(capacity int) {
	self.min = capacity
	self.left = 0
	self.right = 0
	self.capacity = 0
	self.realloc(capacity)
}

func (self *QSArray) Length() int {
	return self.right - self.left
}

func (self *QSArray) Put(v Value) {
	if self.left == self.right {
		if self.capacity == 0 {
			self.realloc(min)
		} else {
			self.left = self.capacity / 3
			self.right = self.left + 1
			self.arr[self.left] = v
		}
	} else {
		if v.Compare(self.arr[self.right-1]) >= 0 {
			if self.right == self.capacity {
				if self.left <= self.capacity>>3 {
					// 空间过小，需要扩容
					self.realloc(self.capacity << 1)
				} else {
					// 左移
					self.move(self.left >> 1)
				}
			}
			self.arr[self.right] = v
			self.right++
		} else if v.Compare(self.arr[self.left]) < 0 {
			if self.left == 0 {
				if self.capacity-self.right <= self.capacity>>3 {
					// 空间过小，需要扩容
					self.realloc(self.capacity << 1)
				} else {
					// 右移
					self.move(self.left + ((self.capacity - self.right) >> 1))
				}
			}
			self.left--
			self.arr[self.left] = v
		} else {
			if self.right == self.capacity {
				if self.left <= self.capacity>>3 {
					// 空间过小，需要扩容
					self.realloc(self.capacity << 1)
				} else {
					// 左移
					self.move(self.left >> 1)
				}
			}
			// search
			left, right := self.left, self.right-1
			for left < right {
				middle := (left + right) >> 1
				if v.Compare(self.arr[middle]) < 0 {
					right = middle - 1
				} else {
					left = middle + 1
				}
			}
			if v.Compare(self.arr[right]) == 0 {
				right++
			}
			copy(self.arr[right+1:], self.arr[right:self.right])
			self.arr[right] = v
			self.right++
		}
	}
}

func (self *QSArray) Shift() Value {
	var v Value
	if self.left < self.right {
		v = self.arr[self.left]
		self.left++
		self.trim()
	}
	return v
}

func (self *QSArray) Pop() Value {
	var v Value
	if self.left < self.right {
		v = self.arr[self.right]
		self.right--
		self.trim()
	}
	return v
}

func (self *QSArray) trim() {
	if self.capacity > self.min && self.capacity > max {
		c := self.right - self.left
		if c < self.capacity>>2 {
			c = ceilToPowerOfTwo(c * 3)
			if c < min {
				c = min
			}
			self.realloc(c)
		}
	}
}

func (self *QSArray) realloc(capacity int) {
	arr := make([]Value, capacity)
	left := capacity / 3
	if self.capacity != 0 {
		copy(arr[left:], self.arr[self.left:self.right])
	}
	self.right += left - self.left
	self.left = left
	self.capacity = capacity
	self.arr = arr
}

func (self *QSArray) move(left int) {
	copy(self.arr[left:], self.arr[self.left:self.right])
	self.right += left - self.left
	self.left = left
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
