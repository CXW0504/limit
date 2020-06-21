package ratelimit

import (
	"encoding/json"
	"sync/atomic"
	"time"
	"unsafe"
)

type Limiter interface {
	TakeAvailableWithNow(now int64) bool
	TakeAvailable() bool
	GetCapacity() int64
	GetLegacyCapacity() int64
}

/**
  漏桶算法限制传输速率。
*/
type leakyBucket struct {
	capacity           int64
	fillInterval       int64
	lastTokenTimestamp unsafe.Pointer
	perRequest         int64
}

func (t *leakyBucket) TakeAvailableWithNow(now int64) bool {
	taken := false
	for !taken {
		var newLast int64 = 0
		previousStatePointer := atomic.LoadPointer(&t.lastTokenTimestamp)
		lastTokenTimestamp := (*int64)(previousStatePointer)

		newLast = *lastTokenTimestamp + t.perRequest

		if now < newLast {
			break
		} else {
			taken = atomic.CompareAndSwapPointer(&t.lastTokenTimestamp, previousStatePointer, unsafe.Pointer(&newLast))
		}
	}
	return taken
}

func (t *leakyBucket) TakeAvailable() bool {
	return t.TakeAvailableWithNow(time.Now().UnixNano())
}

func (t *leakyBucket) GetCapacity() int64 {
	return t.capacity
}
func (t *leakyBucket) GetLegacyCapacity() int64 {
	return -1
}

func (t *leakyBucket) MarshalJSON() ([]byte, error) {
	object := map[string]interface{}{}
	object["capacity"] = t.capacity
	return json.Marshal(object)
}

func NewLeakyBucket(fillInterval time.Duration, capacity int64) Limiter {
	fillIntervalInt := int64(fillInterval)
	l := &leakyBucket{
		fillInterval: fillIntervalInt,
		perRequest:   fillIntervalInt / capacity,
		capacity:     capacity,
	}
	lastTokenTimestamp := time.Now().UnixNano()
	l.lastTokenTimestamp = unsafe.Pointer(&lastTokenTimestamp)
	return l
}

/**
令牌桶算法能够限制突发传输。
*/
type tokenBucket struct {
	capacity        int64
	fillInterval    int64
	tokenBucketStat unsafe.Pointer
	perRequest      int64
}

type tokenBucketStat struct {
	nextTokenTimestamp int64
	keepCapacity       int64 //窗口时间
}

func (t *tokenBucket) TakeAvailableWithNow(now int64) bool {
	taken := false
	for !taken {
		newStat := tokenBucketStat{}
		lastTokenBucketStatPointer := atomic.LoadPointer(&t.tokenBucketStat)
		lastTokenBucketStat := (*tokenBucketStat)(lastTokenBucketStatPointer)

		if now > lastTokenBucketStat.nextTokenTimestamp {
			newStat.nextTokenTimestamp = lastTokenBucketStat.nextTokenTimestamp + t.fillInterval
			newStat.keepCapacity = t.capacity - 1

		} else {

			if lastTokenBucketStat.keepCapacity <= 0 {
				break
			} else {
				newStat.nextTokenTimestamp = lastTokenBucketStat.nextTokenTimestamp
				newStat.keepCapacity = lastTokenBucketStat.keepCapacity - 1
			}
		}
		taken = atomic.CompareAndSwapPointer(&t.tokenBucketStat, lastTokenBucketStatPointer, unsafe.Pointer(&newStat))

	}
	return taken
}

func (t *tokenBucket) TakeAvailable() bool {
	return t.TakeAvailableWithNow(time.Now().UnixNano())
}

func (t *tokenBucket) GetCapacity() int64 {
	return t.capacity
}

func (t *tokenBucket) GetLegacyCapacity() int64 {
	lastTokenBucketStatPointer := atomic.LoadPointer(&t.tokenBucketStat)
	lastTokenBucketStat := (*tokenBucketStat)(lastTokenBucketStatPointer)
	return lastTokenBucketStat.keepCapacity
}

func (t *tokenBucket) MarshalJSON() ([]byte, error) {
	object := map[string]interface{}{}
	object["capacity"] = t.capacity
	lastTokenBucketStatPointer := atomic.LoadPointer(&t.tokenBucketStat)
	lastTokenBucketStat := (*tokenBucketStat)(lastTokenBucketStatPointer)
	object["keepCapacity"] = lastTokenBucketStat.keepCapacity
	return json.Marshal(object)
}

/**
令牌桶算法能够突发传输。
*/
func NewTokenBucket(fillInterval time.Duration, capacity int64) Limiter {
	fillIntervalInt := int64(fillInterval)
	l := &tokenBucket{
		fillInterval: fillIntervalInt,
		capacity:     capacity,
	}
	tokenBucketStat := tokenBucketStat{
		nextTokenTimestamp: time.Now().UnixNano(),
		keepCapacity:       capacity,
	}
	l.tokenBucketStat = unsafe.Pointer(&tokenBucketStat)
	return l
}
