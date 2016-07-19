package btree2d

import (
	"github.com/zenhotels/btree-2d/common"
	"github.com/zenhotels/btree-2d/primary"
	"github.com/zenhotels/btree-2d/secondary"
)

type BTree2D interface {
	Sync(next BTree2D, onAdd, onDel func(key1, key2 common.Comparable))
	GetLayer(key1 common.Comparable) (secondary.Layer, bool)
	SetLayer(key1 common.Comparable, layer secondary.Layer)
	ForEach(fn func(key common.Comparable, layer secondary.Layer) bool)
	ForEach2(key1 common.Comparable, fn func(key2 common.Comparable) bool)
	Put(key1 common.Comparable, key2 common.FinalizableComparable)
	Delete(key1, key2 common.Comparable) bool
	Drop(key1 common.Comparable) bool
}

func NewBTree2D() BTree2D {
	return btree2d{
		primary: primary.NewLayer(),
	}
}

type btree2d struct {
	primary primary.Layer
}

func (prev btree2d) Sync(next BTree2D, onAdd, onDel func(key1, key2 common.Comparable)) {
	nextBTree2D := next.(btree2d)

	switch {
	case onAdd != nil && onDel != nil:
		prev.primary.Sync(nextBTree2D.primary, func(k1 primary.Key, k2 secondary.Key) {
			onAdd(k1.Value, k2.Value)
		}, func(k1 primary.Key, k2 secondary.Key) {
			onDel(k1.Value, k2.Value)
		})
	case onAdd != nil:
		prev.primary.Sync(nextBTree2D.primary, func(k1 primary.Key, k2 secondary.Key) {
			onAdd(k1.Value, k2.Value)
		}, nil)
	case onDel != nil:
		prev.primary.Sync(nextBTree2D.primary, nil, func(k1 primary.Key, k2 secondary.Key) {
			onDel(k1.Value, k2.Value)
		})
	default:
		prev.primary.Sync(nextBTree2D.primary, nil, nil)
	}
}

func (b btree2d) ForEach(fn func(key common.Comparable, layer secondary.Layer) bool) {
	b.primary.ForEach(func(key primary.Key, layer secondary.Layer) bool {
		return fn(key.Value, layer)
	})
}

func (b btree2d) ForEach2(key1 common.Comparable, fn func(key2 common.Comparable) bool) {
	if layer2, ok := b.primary.Get(primary.Key{key1}); ok {
		layer2.ForEach(func(key secondary.Key) bool {
			return fn(key.Value)
		})
	}
}

func (b btree2d) SetLayer(key1 common.Comparable, layer secondary.Layer) {
	b.primary.Set(primary.Key{key1}, layer)
}

func (b btree2d) GetLayer(key1 common.Comparable) (secondary.Layer, bool) {
	return b.primary.Get(primary.Key{key1})
}

func (b btree2d) Drop(key1 common.Comparable) bool {
	return b.primary.Drop(primary.Key{key1})
}

func (b btree2d) Put(key1 common.Comparable, key2 common.FinalizableComparable) {
	b.primary.Put(primary.Key{key1}, secondary.Key{key2})
}

func (b btree2d) Delete(key1, key2 common.Comparable) (ok bool) {
	layer2, ok := b.primary.Get(primary.Key{key1})
	if !ok {
		return false
	}
	fkey := NewFinalizable(key2)
	return layer2.Delete(secondary.Key{fkey})
}
