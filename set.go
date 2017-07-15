package main

type IntSet map[int64]struct{}
// 新建
func NewIntSet() IntSet {
	return make(map[int64]struct{})
}
// 添加
func (set IntSet) Add(v int64) {
	if _, ok := set[v]; ok {
		return
	}
	set[v] = struct{}{}
}
// 是否是群成员
func (set IntSet) IsMember(v int64) bool {
	if _, ok := set[v]; ok {
		return true
	}
	return false
}
// 移除
func (set IntSet) Remove(v int64) {
	if _, ok := set[v]; !ok {
		return
	}
	delete(set, v)
}
// 克隆
func (set IntSet) Clone() IntSet {
	n := make(map[int64]struct{})
	for k, v := range set {
		n[k] = v
	}
	return n
}
