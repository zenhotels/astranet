package btree2d

import "github.com/joeshaw/gengen/generic"

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

func New(cmp1 PrimaryCmpFunc, cmp2 SecondaryCmpFunc) BTree2D {
	return btree2d{
		primary: NewPrimaryLayer(cmp1, cmp2),
	}
}

type btree2d struct {
	primary PrimaryLayer
}

func (prev btree2d) Sync(next BTree2D, onAdd, onDel func(key1 generic.T, key2 generic.U)) {
	nextBTree2D := next.(btree2d)
	prev.primary.Sync(nextBTree2D.primary, onAdd, onDel)
}

func (b btree2d) ForEach(fn func(key generic.T, layer SecondaryLayer) bool) {
	b.primary.ForEach(fn)
}

func (b btree2d) ForEach2(key1 generic.T, fn func(key2 generic.U) bool) {
	if layer2, ok := b.primary.Get(key1); ok {
		layer2.ForEach(func(k generic.U, _ *FinalizerList) bool {
			return fn(k)
		})
	}
}

func (b btree2d) SetLayer(key1 generic.T, layer SecondaryLayer) {
	b.primary.Set(key1, layer)
}

func (b btree2d) GetLayer(key1 generic.T) (SecondaryLayer, bool) {
	return b.primary.Get(key1)
}

func (b btree2d) Drop(key1 generic.T) bool {
	return b.primary.Drop(key1)
}

func (b btree2d) Put(key1 generic.T, key2 generic.U, finalizers ...func()) {
	b.primary.Put(key1, key2, finalizers...)
}

func (b btree2d) Delete(key1 generic.T, key2 generic.U) (ok bool) {
	layer2, ok := b.primary.Get(key1)
	if !ok {
		return false
	}
	return layer2.Delete(key2)
}
