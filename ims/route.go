
package main
import "sync"


type Route struct {
	appid     int64
	mutex     sync.Mutex
	groups    map[int64]*Group
	uids      IntSet
}
// 创建新的路由
func NewRoute(appid int64) *Route {
	r := new(Route)
	r.appid = appid
	r.groups = make(map[int64]*Group)
	r.uids = NewIntSet()
	return r
}
// 是否包含该用户id
func (route *Route) ContainUserID(uid int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	
	return route.uids.IsMember(uid)
}
// 添加用户
func (route *Route) AddUserID(uid int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.uids.Add(uid)
}

// 移除用户
func (route *Route) RemoveUserID(uid int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.uids.Remove(uid)
}
// 添加群成员
func (route *Route) AddGroupMember(gid int64, member int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	if group, ok := route.groups[gid]; ok {
		group.AddMember(member)
	} else {
		members := []int64{member}
		group = NewGroup(gid, route.appid, members)
		route.groups[gid] = group
	}
}
// 移除群成员
func (route *Route) RemoveGroupMember(gid int64, member int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	if group, ok := route.groups[gid]; ok {
		group.RemoveMember(member)
		if group.IsEmpty() {
			delete(route.groups, gid)
		}
	}
}
// 是否包含群成员id
func (route *Route) ContainGroupID(gid int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	_, ok := route.groups[gid]
	return ok
}
// 是否包含群成员
func (route *Route) ContainGroupMember(gid int64, member int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	if group, ok := route.groups[gid]; ok {
		return group.IsMember(member)
	}
	return false
}
