package btree2d

import (
	"io"
	"sync/atomic"

	"github.com/zenhotels/btree-2d/lockie"
	"github.com/zenhotels/btree-2d/util"
)

// SecondaryCmpFunc compares a and b. Return value is:
//
//	< 0 if a <  b
//	  0 if a == b
//	> 0 if a >  b
//
type SecondaryCmpFunc func(key1, key2 string) int

// SecondaryLayer represents the secondary layer,
// a tree holding Finalizable yet Comparable keys.
type SecondaryLayer struct {
	store  *SecondaryTree
	offset uint64
	synced *uint64 // id of the previously synced layer
	lock   lockie.Lockie
	cmp    SecondaryCmpFunc
}

// NewSecondaryLayer initializes a new secondary layer handle.
func NewSecondaryLayer(cmp SecondaryCmpFunc) SecondaryLayer {
	var synced uint64
	return SecondaryLayer{
		synced: &synced,
		store:  NewSecondaryTree(cmp),
		offset: uint64(util.RevOffset()),
		lock:   lockie.NewLockie(),
		cmp:    cmp,
	}
}

func (l SecondaryLayer) Rev() uint64 {
	return l.store.Ver() + l.offset
}

// Put adds finalizers for the key, creating the item if not exists yet.
func (l SecondaryLayer) Put(k string, finalizers ...func()) (added int) {
	l.lock.Lock()
	l.store.Put(k, func(oldV *FinalizerList, exists bool) (newV *FinalizerList, write bool) {
		if !exists || oldV == nil {
			if len(finalizers) == 0 {
				return nil, true
			}
			oldV = &FinalizerList{}
		}
		for i := range finalizers {
			if oldV.AddFinalizer(finalizers[i]) {
				added++
			}
		}
		return oldV, true
	})
	l.lock.Unlock()
	return
}

// ForEach runs the provided function for every element in the layer,
// if function returns true, the loop stops.
func (l SecondaryLayer) ForEach(fn func(key string, val *FinalizerList) bool) {
	l.lock.Lock()
	e, err := l.store.SeekFirst()
	l.lock.Unlock()
	if err != io.EOF {
		k, v, err := e.Next()
		for err != io.EOF {
			if stop := fn(k, v); stop {
				return
			}
			l.lock.Lock()
			k, v, err = e.Next()
			l.lock.Unlock()
		}
		e.Close()
	}
}

// Seek returns an SecondaryEnumerator positioned on a key such that k >= key.
func (l SecondaryLayer) Seek(k string) (e *SecondaryEnumerator, ok bool) {
	l.lock.Lock()
	e, ok = l.store.Seek(k)
	l.lock.Unlock()
	return
}

// SeekFirst returns an SecondaryEnumerator positioned on the first key in the tree.
func (l SecondaryLayer) SeekFirst() (e *SecondaryEnumerator, err error) {
	l.lock.Lock()
	e, err = l.store.SeekFirst()
	l.lock.Unlock()
	return
}

// Delete removes the key and runs all its finalizers.
func (l SecondaryLayer) Delete(k string) (ok bool) {
	l.lock.Lock()
	v, found := l.store.Get(k)
	if found {
		ok = l.store.Delete(k)
	}
	l.lock.Unlock()
	if found && v != nil {
		v.Finalize()
	}
	return
}

func (l SecondaryLayer) close() {
	l.store.Close()
}

// Finalize locks the layer and runs finalizers of all the keys
// from this layer. Call this if you're going to drop an entire layer.
func (l SecondaryLayer) Finalize() {
	l.lock.Lock()
	e, err := l.store.SeekFirst()
	if err != io.EOF {
		_, v, err := e.Next()
		for err != io.EOF {
			if v != nil {
				v.Finalize()
			}
			_, v, err = e.Next()
		}
		e.Close()
	}
	l.lock.Unlock()
}

func (prev SecondaryLayer) Sync(next SecondaryLayer, onAdd, onDel func(key string)) {
	if prev.store == next.store {
		return
	}
	nextRev := next.Rev()
	if prevRev := atomic.LoadUint64(prev.synced); prevRev == nextRev {
		return
	}
	prev.lock.Lock()
	prevIter, prevErr := prev.store.SeekFirst()
	prev.lock.Unlock()
	next.lock.Lock()
	nextIter, nextErr := next.store.SeekFirst()
	next.lock.Unlock()

	switch {
	case prevErr == io.EOF && nextErr == io.EOF:
		// do nothing, both are empty
		atomic.StoreUint64(prev.synced, nextRev)
		return
	case prevErr == io.EOF:
		// previous storage is empty, everything is added
		prev.addAll(next.lock, nextIter, onAdd)
		nextIter.Close()
		atomic.StoreUint64(prev.synced, nextRev)
		return
	case nextErr == io.EOF:
		// next storage is empty, everything is deleted
		prev.deleteAll(prevIter, onDel)
		prevIter.Close()
		atomic.StoreUint64(prev.synced, nextRev)
		return
	default:
		// do sync and trigger the corresponding callbacks
		prev.syncAll(next, prevIter, nextIter, onAdd, onDel)
		prevIter.Close()
		nextIter.Close()
		atomic.StoreUint64(prev.synced, nextRev)
		return
	}
}

func (prev SecondaryLayer) addAll(nextLock lockie.Lockie, nextIter *SecondaryEnumerator, onAdd func(k string)) {
	nextLock.Lock()
	k, _, err := nextIter.Next()
	nextLock.Unlock()

	for err != io.EOF {
		prev.lock.Lock()
		prev.store.Set(k, nil)
		prev.lock.Unlock()
		if onAdd != nil {
			onAdd(k)
		}
		nextLock.Lock()
		k, _, err = nextIter.Next()
		nextLock.Unlock()
	}
}

func (prev SecondaryLayer) deleteAll(prevIter *SecondaryEnumerator, onDel func(k string)) {
	prev.lock.Lock()
	k, v, err := prevIter.Next()
	prev.lock.Unlock()

	for err != io.EOF {
		if onDel != nil {
			onDel(k) // run the callback
		}
		if v != nil {
			v.Finalize() // emit the finalizers
		}

		prev.lock.Lock()
		k, v, err = prevIter.Next()
		prev.lock.Unlock()
	}
	// finally clear the store
	prev.lock.Lock()
	prev.store.Clear()
	prev.lock.Unlock()
}

func (prev SecondaryLayer) syncAll(next SecondaryLayer, prevIter, nextIter *SecondaryEnumerator, onAdd, onDel func(k string)) {
	prev.lock.Lock()
	prevK, prevV, prevErr := prevIter.Next()
	prev.lock.Unlock()
	next.lock.Lock()
	nextK, _, nextErr := nextIter.Next()
	next.lock.Unlock()

	for {
		switch {
		case prevErr == io.EOF:
			if nextErr == io.EOF {
				return // we're done
			}
			// at this point prev is ended, so nextK is added
			if onAdd != nil {
				onAdd(nextK)
			}

			// set nextK into prev
			prev.lock.Lock()
			prev.store.Set(nextK, nil)
			prev.lock.Unlock()
			// move next iterator
			next.lock.Lock()
			nextK, _, nextErr = nextIter.Next()
			next.lock.Unlock()
			continue

		case nextErr == io.EOF:
			if prevErr == io.EOF {
				return // we're done
			}
			// at this point next is ended, so prevK is deleted
			if onDel != nil {
				onDel(prevK)
			}
			if prevV != nil {
				prevV.Finalize()
			}

			// delete prevK in prev
			prev.lock.Lock()
			prev.store.Delete(prevK)
			// move prev iterator
			prevK, _, prevErr = prevIter.Next()
			prev.lock.Unlock()
			continue
		}

		prevCmp := prev.cmp(prevK, nextK)
		switch {
		case prevCmp < 0: // prevK < nextK
			// old prevK has been deleted apparently
			if onDel != nil {
				onDel(prevK)
			}
			if prevV != nil {
				prevV.Finalize()
			}

			// delete prevK in prev
			prev.lock.Lock()
			prev.store.Delete(prevK)
			// move prev iterator
			prevK, _, prevErr = prevIter.Next()
			prev.lock.Unlock()

		case prevCmp > 0: // nextK < prevK
			// new nextK has been inserted apparently
			if onAdd != nil {
				onAdd(nextK)
			}

			// set nextK into prev
			prev.lock.Lock()
			prev.store.Set(nextK, nil)
			prev.lock.Unlock()
			// move next iterator
			next.lock.Lock()
			nextK, _, nextErr = nextIter.Next()
			next.lock.Unlock()

		default:
			// we're on the same keys, move both iterators
			prev.lock.Lock()
			prevK, _, prevErr = prevIter.Next()
			prev.lock.Unlock()
			next.lock.Lock()
			nextK, _, nextErr = nextIter.Next()
			next.lock.Unlock()
		}
	}
}

func (l SecondaryLayer) Len() int {
	l.lock.Lock()
	count := l.store.Len()
	l.lock.Unlock()
	return count
}
