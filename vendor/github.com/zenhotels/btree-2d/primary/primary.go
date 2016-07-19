package primary

import (
	"io"

	"github.com/zenhotels/btree-2d/common"
	"github.com/zenhotels/btree-2d/lockie"
	"github.com/zenhotels/btree-2d/secondary"
)

// Layer represents the primary layer,
// a tree holding Comparable keys pointing to secondary layers.
type Layer struct {
	store  *Tree
	offset uint64
	synced *uint64 // id of the previously synced layer
	lock   lockie.Lockie
}

// NewLayer initializes a new primary layer handle.
func NewLayer() Layer {
	var synced uint64
	return Layer{
		synced: &synced,
		store:  NewTree(treeCmp),
		offset: uint64(common.RevOffset()),
		lock:   lockie.NewLockie(),
	}
}

// TODO(xlab): Set may trigger finalizers?

// Set just adds a secondary layer to the tree, overwriting the previous one.
func (l Layer) Set(k Key, layer secondary.Layer) {
	l.lock.Lock()
	l.store.Set(k, layer)
	l.lock.Unlock()
}

func (l Layer) Rev() uint64 {
	return l.store.Ver() + l.offset
}

// Put adds secondary keys to the secondary layer, if it not exists
// then it will be atomically created.
func (l Layer) Put(k Key, k2 secondary.Key) {
	l.lock.Lock()
	l.store.Put(k, func(oldLayer secondary.Layer, exists bool) (newLayer secondary.Layer, write bool) {
		if !exists {
			oldLayer = secondary.NewLayer()
		}
		oldLayer.Set(k2)
		return oldLayer, true
	})
	l.lock.Unlock()
}

// Seek returns an Enumerator positioned on a secondary layer such that k >= layer's key.
func (l Layer) Seek(k Key) (e *Enumerator, ok bool) {
	l.lock.Lock()
	e, ok = l.store.Seek(k)
	l.lock.Unlock()
	return
}

// SeekFirst returns an Enumerator positioned on the first secondary layer in the tree.
func (l Layer) SeekFirst() (e *Enumerator, err error) {
	l.lock.Lock()
	e, err = l.store.SeekFirst()
	l.lock.Unlock()
	return
}

// ForEach runs the provided function for every element in the layer,
// if function returns true, the loop stops.
func (l Layer) ForEach(fn func(key Key, layer secondary.Layer) bool) {
	l.lock.Lock()
	e, err := l.store.SeekFirst()
	l.lock.Unlock()
	if err != io.EOF {
		k, layer, err := e.Next()
		for err != io.EOF {
			if stop := fn(k, layer); stop {
				return
			}
			l.lock.Lock()
			k, layer, err = e.Next()
			l.lock.Unlock()
		}
		e.Close()
	}
}

// Drop removes the whole secondary layer associated with the key,
// invokes all the finalizers associated with elements of this secondary layer.
func (l Layer) Drop(k Key) (ok bool) {
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

// Get returns the secondary layer associated with the key.
func (l Layer) Get(k Key) (layer secondary.Layer, ok bool) {
	l.lock.Lock()
	v, ok := l.store.Get(k)
	l.lock.Unlock()
	return v, ok
}

func (prev Layer) Sync(next Layer, onAdd, onDel func(key1 Key, key2 secondary.Key)) {
	if prev.store == next.store {
		return
	}
	// TODO(xlab): primary cannot handle changes on secondary layers.
	// Disable this feature for now
	//
	// nextRev := next.Rev()
	// if prevRev := atomic.LoadUint64(prev.synced); prevRev == nextRev {
	// 	log.Println()
	// 	return
	// }
	prev.lock.Lock()
	prevIter, prevErr := prev.store.SeekFirst()
	prev.lock.Unlock()
	next.lock.Lock()
	nextIter, nextErr := next.store.SeekFirst()
	next.lock.Unlock()

	switch {
	case prevErr == io.EOF && nextErr == io.EOF:
		// do nothing, both are empty
		return
	case prevErr == io.EOF:
		// previous storage is empty, everything is added
		addAll(prev, next.lock, nextIter, onAdd)
		nextIter.Close()
		return
	case nextErr == io.EOF:
		// next storage is empty, everything is deleted
		deleteAll(prev, prev.lock, prevIter, onDel)
		prevIter.Close()
		return
	default:
		// do sync and trigger the corresponding callbacks
		syncAll(prev, next, prevIter, nextIter, onAdd, onDel)
		prevIter.Close()
		nextIter.Close()
		return
	}
}

func addAll(prev Layer, nextLock lockie.Lockie, nextIter *Enumerator,
	onAdd func(key1 Key, key2 secondary.Key)) {

	nextLock.Lock()
	nextK, nextLayer, err := nextIter.Next()
	nextLock.Unlock()

	for err != io.EOF {
		if nextLayer.Len() > 0 {
			// create a new layer to set into prev
			newLayer := secondary.NewLayer()

			// fills layer while calling the onAdd callback
			if onAdd != nil {
				newLayer.Sync(nextLayer, func(k2 secondary.Key) {
					onAdd(nextK, k2)
				}, nil)
			} else {
				newLayer.Sync(nextLayer, nil, nil)
			}

			// set the new layer into prev
			prev.lock.Lock()
			prev.store.Set(nextK, newLayer)
			prev.lock.Unlock()
		}
		// advance next iter
		nextLock.Lock()
		nextK, nextLayer, err = nextIter.Next()
		nextLock.Unlock()
	}
}

func deleteAll(prev Layer, prevLock lockie.Lockie, prevIter *Enumerator,
	onDel func(key1 Key, key2 secondary.Key)) {

	prevLock.Lock()
	prevK, prevLayer, err := prevIter.Next()
	prevLock.Unlock()

	for err != io.EOF {
		// nukes the prevLayer yet calling the onDel callback
		if onDel != nil {
			prevLayer.Sync(secondary.NewLayer(), nil, func(k2 secondary.Key) {
				onDel(prevK, k2)
			})
		} else {
			prevLayer.Sync(secondary.NewLayer(), nil, nil)
		}

		// advance next iter
		prevLock.Lock()
		prevK, prevLayer, err = prevIter.Next()
		prevLock.Unlock()
	}
	// finally clear the store
	prevLock.Lock()
	prev.store.Clear()
	prevLock.Unlock()
}

func syncAll(prev, next Layer, prevIter, nextIter *Enumerator,
	onAdd, onDel func(k1 Key, k2 secondary.Key)) {

	prev.lock.Lock()
	prevK, prevLayer, prevErr := prevIter.Next()
	prev.lock.Unlock()
	next.lock.Lock()
	nextK, nextLayer, nextErr := nextIter.Next()
	next.lock.Unlock()

	for {
		switch {
		case prevErr == io.EOF:
			if nextErr == io.EOF {
				return // we're done
			}
			// at this point prev is ended, so nextK is added
			if nextLayer.Len() > 0 {
				// create a new layer to set into prev
				newLayer := secondary.NewLayer()

				// fills layer while calling the onAdd callback
				if onAdd != nil {
					newLayer.Sync(nextLayer, func(k2 secondary.Key) {
						onAdd(nextK, k2)
					}, nil)
				} else {
					newLayer.Sync(nextLayer, nil, nil)
				}

				// set the new layer into prev
				prev.lock.Lock()
				prev.store.Set(nextK, newLayer)
				prev.lock.Unlock()
			}
			// move next iterator
			next.lock.Lock()
			nextK, nextLayer, nextErr = nextIter.Next()
			next.lock.Unlock()

		case nextErr == io.EOF:
			if prevErr == io.EOF {
				return // we're done
			}
			// at this point next is ended, so prevK is deleted
			if onDel != nil {
				prevLayer.ForEach(func(k2 secondary.Key) bool {
					if onDel != nil {
						onDel(prevK, k2)
					}
					k2.Finalize()
					return false
				})
			} else {
				prevLayer.Finalize()
			}
			// delete prevK in prev
			prev.lock.Lock()
			prev.store.Delete(prevK)
			// move prev iterator
			prevK, prevLayer, prevErr = prevIter.Next()
			prev.lock.Unlock()

		case prevK.Less(nextK):
			// old prevK has been deleted apparently
			if onDel != nil {
				prevLayer.ForEach(func(k2 secondary.Key) bool {
					if onDel != nil {
						onDel(prevK, k2)
					}
					k2.Finalize()
					return false
				})
			} else {
				prevLayer.Finalize()
			}

			// delete prevK in prev
			prev.lock.Lock()
			prev.store.Delete(prevK)
			// move prev iterator
			prevK, prevLayer, prevErr = prevIter.Next()
			prev.lock.Unlock()

		case nextK.Less(prevK):
			// new nextK has been inserted apparently
			if nextLayer.Len() > 0 {
				// create a new layer to set into prev
				newLayer := secondary.NewLayer()

				// fills layer while calling the onAdd callback
				if onAdd != nil {
					newLayer.Sync(nextLayer, func(k2 secondary.Key) {
						onAdd(nextK, k2)
					}, nil)
				} else {
					newLayer.Sync(nextLayer, nil, nil)
				}

				// set the new layer into prev
				prev.lock.Lock()
				prev.store.Set(nextK, newLayer)
				prev.lock.Unlock()
			}
			// move next iterator
			next.lock.Lock()
			nextK, nextLayer, nextErr = nextIter.Next()
			next.lock.Unlock()

		default:
			// we're on the same keys, sync the layers
			switch {
			case onAdd != nil && onDel != nil:
				prevLayer.Sync(nextLayer, func(k2 secondary.Key) {
					onAdd(nextK, k2)
				}, func(k2 secondary.Key) {
					onDel(prevK, k2)
				})
			case onAdd != nil:
				prevLayer.Sync(nextLayer, func(k2 secondary.Key) {
					onAdd(nextK, k2)
				}, nil)
			case onDel != nil:
				prevLayer.Sync(nextLayer, nil, func(k2 secondary.Key) {
					onDel(prevK, k2)
				})
			default: // no callbacks
				prevLayer.Sync(nextLayer, nil, nil)
			}

			// move both iterators
			prev.lock.Lock()
			next.lock.Lock()
			prevK, prevLayer, prevErr = prevIter.Next()
			nextK, nextLayer, nextErr = nextIter.Next()
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
