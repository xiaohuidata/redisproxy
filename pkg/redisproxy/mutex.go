package redisproxy

// github.com/go-redsync/redsync
//

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

type DelayFunc func(tries int) time.Duration

const (
	minRetryDelayMilliSec = 50
	maxRetryDelayMilliSec = 250
)

type Mutex struct {
	name   string
	expiry time.Duration

	tries     int
	delayFunc DelayFunc

	driftFactor   float64
	timeoutFactor float64

	genValueFunc func() (string, error)
	value        string
	until        time.Time

	conn *ClientProxy
}

func (m *Mutex) Name() string {
	return m.name
}

func (m *Mutex) Value() string {
	return m.value
}

func (m *Mutex) Until() time.Time {
	return m.until
}

func (m *Mutex) Lock() error {
	return m.LockContext(nil)
}

func (m *Mutex) LockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	value, err := m.genValueFunc()
	if err != nil {
		return err
	}

	for i := 0; i < m.tries; i++ {
		if i != 0 {
			select {
			case <-ctx.Done():
				return ErrFailed
			case <-time.After(m.delayFunc(i)):
			}
		}

		start := time.Now()

		n, err := func() (bool, error) {
			_, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(m.expiry)*m.timeoutFactor)))
			defer cancel()
			return m.acquire(ctx, value)
		}()

		if n && err != nil {
			return err
		}

		now := time.Now()
		until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
		if n && now.Before(until) {
			m.value = value
			m.until = until
			return nil
		}

		_, err = func() (bool, error) {
			ctx, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(m.expiry)*m.timeoutFactor)))
			defer cancel()
			return m.release(ctx, value)
		}()
		if i == m.tries-1 && err != nil {
			return err
		}
	}
	return nil
}

func (m *Mutex) Unlock() (bool, error) {
	return m.UnlockContext(nil)
}

func (m *Mutex) UnlockContext(ctx context.Context) (bool, error) {
	return m.release(ctx, m.value)
}

func (m *Mutex) Extend() (bool, error) {
	return m.ExtendContext(nil)
}

func (m *Mutex) ExtendContext(ctx context.Context) (bool, error) {
	start := time.Now()
	n, err := m.touch(ctx, m.value, int(m.expiry/time.Millisecond))
	if !n {
		return false, err
	}
	now := time.Now()
	until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
	if now.Before(until) {
		m.until = until
		return true, nil
	}
	return false, ErrExtendFailed
}

func (m *Mutex) Valid() (bool, error) {
	return m.ValidContext(nil)
}

func (m *Mutex) ValidContext(ctx context.Context) (bool, error) {
	return m.valid(ctx)
}

func (m *Mutex) valid(ctx context.Context) (bool, error) {
	if m.value == "" {
		return false, nil
	}
	reply, err := redis.String((*m.conn).Do("GET", m.name))
	return m.value == reply, err
}

func genValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (m *Mutex) acquire(ctx context.Context, value string) (bool, error) {
	reply, err := redis.String((*m.conn).Do("SET", m.name, value, "NX", "PX", int(m.expiry/time.Millisecond)))
	return reply == "OK", err
}

var deleteScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`

func (m *Mutex) release(ctx context.Context, value string) (bool, error) {
	script := (*m.conn).NewScript(1, deleteScript)
	status, err := redis.Int64(script.Do(m.name, value))
	fmt.Println("release", status, err)
	return err == nil && status != 0, err
}

var touchScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`

func (m *Mutex) touch(ctx context.Context, value string, expiry int) (bool, error) {
	script := (*m.conn).NewScript(1, touchScript)
	(*m.conn).Flush()
	status, err := redis.Int64(script.Do(m.name, value, expiry))
	return err == nil && status != 0, err
}
