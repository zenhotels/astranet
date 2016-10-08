package dumbmap2d

import (
	"sync"

	"github.com/joeshaw/gengen/generic"
)

type BTree2D interface {
	Sync(next BTree2D, onAdd, onDel func(key1 generic.T, key2 generic.U))
	GetLayer(key1 generic.T) (SecondaryLayer, bool)
	SetLayer(key1 generic.T, layer SecondaryLayer)
	ForEach(fn func(key generic.T, layer SecondaryLayer) bool)
	ForEach2(key1 generic.T, fn func(key2 generic.U) bool)
	Put(key1 generic.T, key2 generic.U, finalizers ...func())
	Delete(key1 generic.T, key2 generic.U) bool
	Drop(key1 generic.T) bool
}

func New() BTree2D {
	return &dumbmap2d{
		lock:  new(sync.RWMutex),
		store: make(map[generic.T]*SecondaryLayer, 256),
	}
}

type dumbmap2d struct {
	lock  *sync.RWMutex
	store map[generic.T]*SecondaryLayer
}

func (prev *dumbmap2d) Sync(nextMap BTree2D, onAdd, onDel func(key1 generic.T, key2 generic.U)) {
	next := nextMap.(*dumbmap2d)
	prev.lock.Lock()
	defer prev.lock.Unlock()
	next.lock.Lock()
	defer next.lock.Unlock()

	for key, layer := range prev.store {
		if _, ok := next.store[key]; !ok {
			// deleted in the next revision
			delete(prev.store, key)
			if onDel != nil {
				layer.Sync(NewSecondaryLayer(), nil, func(key2 generic.U) {
					onDel(key, key2)
				})
			} else {
				layer.Sync(NewSecondaryLayer(), nil, nil)
			}
		}
	}
	for key, layer := range next.store {
		if _, ok := prev.store[key]; !ok {
			// added in the next revision
			newLayer := NewSecondaryLayer()
			if onAdd != nil {
				newLayer.Sync(layer, nil, func(key2 generic.U) {
					onAdd(key, key2)
				})
			} else {
				newLayer.Sync(layer, nil, nil)
			}
			prev.store[key] = newLayer
		}
	}
}

var emptyLayer = SecondaryLayer{}

func (b *dumbmap2d) ForEach(fn func(key generic.T, layer SecondaryLayer) bool) {
	b.lock.RLock()
	for key, layer := range b.store {
		if layer != nil {
			fn(key, *layer)
		} else {
			fn(key, emptyLayer)
		}
	}
	b.lock.RUnlock()
}

func (b *dumbmap2d) ForEach2(key1 generic.T, fn func(key2 generic.U) bool) {
	b.lock.RLock()
	if layer, ok := b.store[key1]; ok {
		layer.ForEach(func(k generic.U, _ *FinalizerList) bool {
			return fn(k)
		})
	}
	b.lock.RUnlock()
}

func (b *dumbmap2d) SetLayer(key1 generic.T, layer SecondaryLayer) {
	panic("not implemented")
}

func (b *dumbmap2d) GetLayer(key1 generic.T) (SecondaryLayer, bool) {
	panic("not implemented")
}

func (b *dumbmap2d) Drop(key1 generic.T) (ok bool) {
	b.lock.Lock()
	var layer *SecondaryLayer
	layer, ok = b.store[key1]
	if ok && layer != nil {
		layer.Finalize()
	}
	delete(b.store, key1)
	b.lock.Unlock()
	return
}

func (b *dumbmap2d) Put(key1 generic.T, key2 generic.U, finalizers ...func()) {
	b.lock.Lock()
	layer, ok := b.store[key1]
	if !ok || layer == nil {
		layer = NewSecondaryLayer()
		b.store[key1] = layer
	}
	layer.Put(key2, finalizers...)
	b.lock.Unlock()
}

func (b *dumbmap2d) Delete(key1 generic.T, key2 generic.U) (ok bool) {
	b.lock.Lock()
	var layer *SecondaryLayer
	layer, ok = b.store[key1]
	if ok && layer != nil {
		ok = layer.Delete(key2)
		if layer.Len() == 0 {
			delete(b.store, key1)
		}
	}
	b.lock.Unlock()
	return
}
