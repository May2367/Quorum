package statemachine

import "errors"

var (
	ErrLockHeld        = errors.New("lock is held")
	ErrLockNotHeld     = errors.New("lock is not held")
	ErrLockNotOwner    = errors.New("lock is owned by another holder")
	ErrInvalidLeaseTTL = errors.New("lease ttl must be positive")
	ErrInvalidFence    = errors.New("fencing token must be positive")
)

type LockTable struct {
	locks map[string]lockState
	tick  uint64
}

type lockState struct {
	holder       string
	fencingToken uint64
	expiresAt    uint64
}

type LockResult struct {
	Name         string
	Holder       string
	FencingToken uint64
	ExpiresAt    uint64
	Acquired     bool
}

func NewLockTable() *LockTable {
	return &LockTable{locks: map[string]lockState{}}
}

func (l *LockTable) Acquire(name, holder string, ttlTicks, fencingToken uint64) (LockResult, error) {
	if ttlTicks == 0 {
		return LockResult{}, ErrInvalidLeaseTTL
	}
	if fencingToken == 0 {
		return LockResult{}, ErrInvalidFence
	}
	l.expireLocked()
	cur, ok := l.locks[name]
	if ok && cur.holder != holder {
		return LockResult{Name: name, Holder: cur.holder, FencingToken: cur.fencingToken, ExpiresAt: cur.expiresAt}, ErrLockHeld
	}
	if ok && cur.fencingToken > fencingToken {
		return LockResult{Name: name, Holder: cur.holder, FencingToken: cur.fencingToken, ExpiresAt: cur.expiresAt}, ErrStaleFencingToken
	}
	state := lockState{holder: holder, fencingToken: fencingToken, expiresAt: l.tick + ttlTicks}
	l.locks[name] = state
	return LockResult{Name: name, Holder: holder, FencingToken: fencingToken, ExpiresAt: state.expiresAt, Acquired: true}, nil
}

func (l *LockTable) KeepAlive(name, holder string, ttlTicks uint64) (LockResult, error) {
	if ttlTicks == 0 {
		return LockResult{}, ErrInvalidLeaseTTL
	}
	l.expireLocked()
	cur, ok := l.locks[name]
	if !ok {
		return LockResult{Name: name}, ErrLockNotHeld
	}
	if cur.holder != holder {
		return LockResult{Name: name, Holder: cur.holder, FencingToken: cur.fencingToken, ExpiresAt: cur.expiresAt}, ErrLockNotOwner
	}
	cur.expiresAt = l.tick + ttlTicks
	l.locks[name] = cur
	return LockResult{Name: name, Holder: cur.holder, FencingToken: cur.fencingToken, ExpiresAt: cur.expiresAt, Acquired: true}, nil
}

func (l *LockTable) Release(name, holder string) error {
	l.expireLocked()
	cur, ok := l.locks[name]
	if !ok {
		return ErrLockNotHeld
	}
	if cur.holder != holder {
		return ErrLockNotOwner
	}
	delete(l.locks, name)
	return nil
}

func (l *LockTable) Advance(ticks uint64) {
	l.tick += ticks
	l.expireLocked()
}

func (l *LockTable) Tick() uint64 {
	return l.tick
}

func (l *LockTable) Get(name string) (LockResult, bool) {
	l.expireLocked()
	cur, ok := l.locks[name]
	if !ok {
		return LockResult{Name: name}, false
	}
	return LockResult{Name: name, Holder: cur.holder, FencingToken: cur.fencingToken, ExpiresAt: cur.expiresAt, Acquired: true}, true
}

func (l *LockTable) expireLocked() {
	for name, cur := range l.locks {
		if cur.expiresAt <= l.tick {
			delete(l.locks, name)
		}
	}
}
