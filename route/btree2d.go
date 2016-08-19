package route

type BTree2D interface {
	Sync(next BTree2D, onAdd, onDel func(key1 uint64, key2 RouteInfo))
	GetLayer(key1 uint64) (SecondaryLayer, bool)
	SetLayer(key1 uint64, layer SecondaryLayer)
	ForEach(fn func(key uint64, layer SecondaryLayer) bool)
	ForEach2(key1 uint64, fn func(key2 RouteInfo) bool)
	Put(key1 uint64, key2 RouteInfo, finalizers ...func())
	Delete(key1 uint64, key2 RouteInfo) bool
	Drop(key1 uint64) bool
}

func New(cmp1 PrimaryCmpFunc, cmp2 SecondaryCmpFunc) BTree2D {
	return btree2d{
		primary: NewPrimaryLayer(cmp1, cmp2),
	}
}

type btree2d struct {
	primary PrimaryLayer
}

func (prev btree2d) Sync(next BTree2D, onAdd, onDel func(key1 uint64, key2 RouteInfo)) {
	nextBTree2D := next.(btree2d)
	prev.primary.Sync(nextBTree2D.primary, onAdd, onDel)
}

func (b btree2d) ForEach(fn func(key uint64, layer SecondaryLayer) bool) {
	b.primary.ForEach(fn)
}

func (b btree2d) ForEach2(key1 uint64, fn func(key2 RouteInfo) bool) {
	if layer2, ok := b.primary.Get(key1); ok {
		layer2.ForEach(func(k RouteInfo, _ *FinalizerList) bool {
			return fn(k)
		})
	}
}

func (b btree2d) SetLayer(key1 uint64, layer SecondaryLayer) {
	b.primary.Set(key1, layer)
}

func (b btree2d) GetLayer(key1 uint64) (SecondaryLayer, bool) {
	return b.primary.Get(key1)
}

func (b btree2d) Drop(key1 uint64) bool {
	return b.primary.Drop(key1)
}

func (b btree2d) Put(key1 uint64, key2 RouteInfo, finalizers ...func()) {
	b.primary.Put(key1, key2, finalizers...)
}

func (b btree2d) Delete(key1 uint64, key2 RouteInfo) (ok bool) {
	layer2, ok := b.primary.Get(key1)
	if !ok {
		return false
	}
	return layer2.Delete(key2)
}
