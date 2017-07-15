package main

import "sync"
import (
	log "github.com/golang/glog"
	"fmt"
)


type Route struct {
	appid  int64
	mutex   sync.Mutex
	clients map[int64]ClientSet
	room_clients map[int64]ClientSet
}

func NewRoute(appid int64) *Route {
	fmt.Println("新建一个路由")
	route := new(Route)
	route.appid = appid
	route.clients = make(map[int64]ClientSet)
	route.room_clients = make(map[int64]ClientSet)
	return route
}

func (route *Route) AddRoomClient(room_id int64, client *Client) {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	set, ok := route.room_clients[room_id]; 
	if !ok {
		set = NewClientSet()
		route.room_clients[room_id] = set
	}
	set.Add(client)
}

//todo optimise client set clone
func (route *Route) FindRoomClientSet(room_id int64) ClientSet {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	set, ok := route.room_clients[room_id]
	if ok {
		return set.Clone()
	} else {
		return nil
	}
}

func (route *Route) RemoveRoomClient(room_id int64, client *Client) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	if set, ok := route.room_clients[room_id]; ok {
		set.Remove(client)
		if set.Count() == 0 {
			delete(route.room_clients, room_id)
		}
		return true
	}
	fmt.Println("room client non exists")
	log.Info("room client non exists")
	return false
}

func (route *Route) AddClient(client *Client) {
	fmt.Println("给route添加客户端")
	route.mutex.Lock()
	defer route.mutex.Unlock()
	set, ok := route.clients[client.uid]; 
	if !ok {
		set = NewClientSet()
		route.clients[client.uid] = set
	}
	set.Add(client)
}

func (route *Route) RemoveClient(client *Client) bool {
	fmt.Println("移除route中的客户端")
	route.mutex.Lock()
	defer route.mutex.Unlock()
	if set, ok := route.clients[client.uid]; ok {
		set.Remove(client)
		if set.Count() == 0 {
			delete(route.clients, client.uid)
		}
		return true
	}
	fmt.Println("client non exists")
	log.Info("client non exists")
	return false
}

func (route *Route) FindClientSet(uid int64) ClientSet {
	fmt.Println("在route中查找某一个客户端")
	route.mutex.Lock()
	defer route.mutex.Unlock()

	set, ok := route.clients[uid]
	if ok {
		return set.Clone()
	} else {
		return nil
	}
}

func (route *Route) IsOnline(uid int64) bool {
	fmt.Println("查看路由中的客户端是否在线")
	route.mutex.Lock()
	defer route.mutex.Unlock()

	set, ok := route.clients[uid]
	if ok {
		return len(set) > 0
	}
	return false
}

