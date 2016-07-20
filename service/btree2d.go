package service

type BTree2D interface {
	Sync(next BTree2D, onAdd, onDel func(key1 string, key2 ServiceInfo))
	GetLayer(key1 string) (SecondaryLayer, bool)
	SetLayer(key1 string, layer SecondaryLayer)
	ForEach(fn func(key string, layer SecondaryLayer) bool)
	ForEach2(key1 string, fn func(key2 ServiceInfo) bool)
	Put(key1 string, key2 ServiceInfo, finalizers ...func())
	Delete(key1 string, key2 ServiceInfo) bool
	Drop(key1 string) bool
}

func New(cmp1 PrimaryCmpFunc, cmp2 SecondaryCmpFunc) BTree2D {
	return btree2d{
		primary: NewPrimaryLayer(cmp1, cmp2),
	}
}

type btree2d struct {
	primary PrimaryLayer
}

func (prev btree2d) Sync(next BTree2D, onAdd, onDel func(key1 string, key2 ServiceInfo)) {
	nextBTree2D := next.(btree2d)
	prev.primary.Sync(nextBTree2D.primary, onAdd, onDel)
}

func (b btree2d) ForEach(fn func(key string, layer SecondaryLayer) bool) {
	b.primary.ForEach(fn)
}

func (b btree2d) ForEach2(key1 string, fn func(key2 ServiceInfo) bool) {
	if layer2, ok := b.primary.Get(key1); ok {
		layer2.ForEach(func(k ServiceInfo, _ *FinalizerList) bool {
			return fn(k)
		})
	}
}

func (b btree2d) SetLayer(key1 string, layer SecondaryLayer) {
	b.primary.Set(key1, layer)
}

func (b btree2d) GetLayer(key1 string) (SecondaryLayer, bool) {
	return b.primary.Get(key1)
}

func (b btree2d) Drop(key1 string) bool {
	return b.primary.Drop(key1)
}

func (b btree2d) Put(key1 string, key2 ServiceInfo, finalizers ...func()) {
	b.primary.Put(key1, key2, finalizers...)
}

func (b btree2d) Delete(key1 string, key2 ServiceInfo) (ok bool) {
	layer2, ok := b.primary.Get(key1)
	if !ok {
		return false
	}
	return layer2.Delete(key2)
}
