package main

import (
	"sync"
	"fmt"
)

type AppRoute struct {
	mutex sync.Mutex
	apps  map[int64]*Route
}
// 创建新的app路由
func NewAppRoute() *AppRoute {
	fmt.Println("新建一个APP路由")
	app_route := new(AppRoute)
	app_route.apps = make(map[int64]*Route)
	return app_route
}

// 查找或添加路由
func (app_route *AppRoute) FindOrAddRoute(appid int64) *Route {
	fmt.Println("查找或添加一个APP路由")
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()
	if route, ok := app_route.apps[appid]; ok {
		return route
	}
	route := NewRoute(appid)
	app_route.apps[appid] = route
	return route
}

// 查找路由
func (app_route *AppRoute) FindRoute(appid int64) *Route{
	fmt.Println("查找的appid ->", appid)
	fmt.Println("查找APP路由")
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()
	fmt.Println("查找APP路由-> ", app_route.apps)
	return app_route.apps[appid]
}

// 添加路由
func (app_route *AppRoute) AddRoute(route *Route) {
	fmt.Println("添加APP路由")
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()
	app_route.apps[route.appid] = route
}

type ClientSet map[*Client]struct{}

// 创建新的客户端集合
func NewClientSet() ClientSet {
	fmt.Println("新建路由中的客户端集合")
	return make(map[*Client]struct{})
}
// 添加客户端
func (set ClientSet) Add(c *Client) {
	set[c] = struct{}{}
}
// 判断是否是客户端成员
func (set ClientSet) IsMember(c *Client) bool {
	if _, ok := set[c]; ok {
		return true
	}
	return false
}
// 移除客户端
func (set ClientSet) Remove(c *Client) {
	if _, ok := set[c]; !ok {
		return
	}
	delete(set, c)
}
// 计数
func (set ClientSet) Count() int {
	return len(set)
}
// 克隆
func (set ClientSet) Clone() ClientSet {
	n := make(map[*Client]struct{})
	for k, v := range set {
		n[k] = v
	}
	return n
}
