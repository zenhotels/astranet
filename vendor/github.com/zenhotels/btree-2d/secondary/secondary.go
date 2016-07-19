package secondary

import (
	"io"
	"sync/atomic"

	"github.com/zenhotels/btree-2d/common"
	"github.com/zenhotels/btree-2d/lockie"
)

// Layer represents the secondary layer,
// a tree holding Finalizable yet Comparable keys.
type Layer struct {
	store  *Tree
	offset uint64
	synced *uint64 // id of the previously synced layer
	lock   lockie.Lockie
}

// NewLayer initializes a new secondary layer handle.
func NewLayer() Layer {
	var synced uint64
	return Layer{
		synced: &synced,
		store:  NewTree(treeCmp),
		offset: uint64(common.RevOffset()),
		lock:   lockie.NewLockie(),
	}
}

func (l Layer) Rev() uint64 {
	return l.store.Ver() + l.offset
}

// TODO(xlab): Set may trigger finalizers?

// Set just adds a key to the tree, overwriting the previous one.
func (l Layer) Set(k Key) {
	l.lock.Lock()
	l.store.Set(k, k)
	l.lock.Unlock()
}

// ForEach runs the provided function for every element in the layer,
// if function returns true, the loop stops.
func (l Layer) ForEach(fn func(key Key) bool) {
	l.lock.Lock()
	e, err := l.store.SeekFirst()
	l.lock.Unlock()
	if err != io.EOF {
		k, _, err := e.Next()
		for err != io.EOF {
			if stop := fn(k); stop {
				return
			}
			l.lock.Lock()
			k, _, err = e.Next()
			l.lock.Unlock()
		}
		e.Close()
	}
}

// Seek returns an Enumerator positioned on a key such that k >= key.
func (l Layer) Seek(k Key) (e *Enumerator, ok bool) {
	l.lock.Lock()
	e, ok = l.store.Seek(k)
	l.lock.Unlock()
	return
}

// SeekFirst returns an Enumerator positioned on the first key in the tree.
func (l Layer) SeekFirst() (e *Enumerator, err error) {
	l.lock.Lock()
	e, err = l.store.SeekFirst()
	l.lock.Unlock()
	return
}

// Delete removes the key and runs all its finalizers.
func (l Layer) Delete(k Key) (ok bool) {
	l.lock.Lock()
	v, found := l.store.Get(k)
	if found {
		ok = l.store.Delete(k)
	}
	l.lock.Unlock()
	if found {
		v.Finalize()
	}
	return
}

// Finalize locks the layer and runs finalizers of all the keys
// from this layer. Call this if you're going to drop an entire layer.
func (l Layer) Finalize() {
	l.lock.Lock()
	e, err := l.store.SeekFirst()
	if err != io.EOF {
		k, _, err := e.Next()
		for err != io.EOF {
			k.Finalize()
			k, _, err = e.Next()
		}
		e.Close()
	}
	l.lock.Unlock()
}

func (prev Layer) Sync(next Layer, onAdd, onDel func(key Key)) {
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
		addAll(prev, next.lock, nextIter, onAdd)
		nextIter.Close()
		atomic.StoreUint64(prev.synced, nextRev)
		return
	case nextErr == io.EOF:
		// next storage is empty, everything is deleted
		deleteAll(prev, prev.lock, prevIter, onDel)
		prevIter.Close()
		atomic.StoreUint64(prev.synced, nextRev)
		return
	default:
		// do sync and trigger the corresponding callbacks
		syncAll(prev, next, prevIter, nextIter, onAdd, onDel)
		prevIter.Close()
		nextIter.Close()
		atomic.StoreUint64(prev.synced, nextRev)
		return
	}
}

func addAll(prev Layer, nextLock lockie.Lockie, nextIter *Enumerator, onAdd func(k Key)) {
	nextLock.Lock()
	k, _, err := nextIter.Next()
	nextLock.Unlock()

	for err != io.EOF {
		prev.lock.Lock()
		prev.store.Set(k, k)
		prev.lock.Unlock()
		if onAdd != nil {
			onAdd(k)
		}
		nextLock.Lock()
		k, _, err = nextIter.Next()
		nextLock.Unlock()
	}
}

func deleteAll(prev Layer, prevLock lockie.Lockie, prevIter *Enumerator, onDel func(k Key)) {
	prevLock.Lock()
	k, _, err := prevIter.Next()
	prevLock.Unlock()

	for err != io.EOF {
		if onDel != nil {
			onDel(k) // run the callback
		}
		k.Finalize() // emit the finalizers

		prevLock.Lock()
		k, _, err = prevIter.Next()
		prevLock.Unlock()
	}
	// finally clear the store
	prevLock.Lock()
	prev.store.Clear()
	prevLock.Unlock()
}

func syncAll(prev, next Layer, prevIter, nextIter *Enumerator, onAdd, onDel func(k Key)) {
	prev.lock.Lock()
	prevK, _, prevErr := prevIter.Next()
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
			prev.store.Set(nextK, nextK)
			prev.lock.Unlock()
			// move next iterator
			next.lock.Lock()
			nextK, _, nextErr = nextIter.Next()
			next.lock.Unlock()

		case nextErr == io.EOF:
			if prevErr == io.EOF {
				return // we're done
			}
			// at this point next is ended, so prevK is deleted
			if onDel != nil {
				onDel(prevK)
			}
			prevK.Finalize()

			// delete prevK in prev
			prev.lock.Lock()
			prev.store.Delete(prevK)
			// move prev iterator
			prevK, _, prevErr = prevIter.Next()
			prev.lock.Unlock()

		case prevK.Less(nextK):
			// old prevK has been deleted apparently
			if onDel != nil {
				onDel(prevK)
			}
			prevK.Finalize()

			// delete prevK in prev
			prev.lock.Lock()
			prev.store.Delete(prevK)
			// move prev iterator
			prevK, _, prevErr = prevIter.Next()
			prev.lock.Unlock()

		case nextK.Less(prevK):
			// new nextK has been inserted apparently
			if onAdd != nil {
				onAdd(nextK)
			}

			// set nextK into prev
			prev.lock.Lock()
			prev.store.Set(nextK, nextK)
			prev.lock.Unlock()
			// move next iterator
			next.lock.Lock()
			nextK, _, nextErr = nextIter.Next()
			next.lock.Unlock()

		default:
			// we're on the same keys, move both iterators
			prev.lock.Lock()
			next.lock.Lock()
			prevK, _, prevErr = prevIter.Next()
			nextK, _, nextErr = nextIter.Next()
			next.lock.Unlock()
			prev.lock.Unlock()
		}
	}
}

func (l Layer) Len() int {
	l.lock.Lock()
	count := l.store.Len()
	l.lock.Unlock()
	return count
}

func treeCmp(k1, k2 Key) int {
	if k1.Less(k2) {
		return -1
	} else if k2.Less(k1) {
		return 1
	}
	return 0
}
