package main

import "net"
import "time"
import "sync"
import (
	log "github.com/golang/glog"
	"fmt"
)

type Subscriber struct {
	uids map[int64]int
	room_ids map[int64]int
}

func NewSubscriber() *Subscriber {
	s := new(Subscriber)
	s.uids = make(map[int64]int)
	s.room_ids = make(map[int64]int)
	return s
}

type Channel struct {
	addr            string
	wt              chan *Message

	mutex           sync.Mutex
	subscribers     map[int64]*Subscriber

	dispatch        func(*AppMessage)
	dispatch_group  func(*AppMessage)
	dispatch_room   func(*AppMessage)

}

func NewChannel(addr string, f func(*AppMessage), 
	f2 func(*AppMessage), f3 func(*AppMessage)) *Channel {
	fmt.Println("新建一个channel")
	channel := new(Channel)
	channel.subscribers = make(map[int64]*Subscriber)
	channel.dispatch = f
	channel.dispatch_group = f2
	channel.dispatch_room = f3
	channel.addr = addr
	channel.wt = make(chan *Message, 10)
	return channel
}

//返回添加前的计数
func (channel *Channel) AddSubscribe(appid, uid int64) int {
	fmt.Println("添加一个订阅者")
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		subscriber = NewSubscriber()
		channel.subscribers[appid] = subscriber
	}
	//不存在count==0
	count := subscriber.uids[uid]
	subscriber.uids[uid] = count + 1
	return count
}

//返回删除前的计数
func (channel *Channel) RemoveSubscribe(appid, uid int64) int {
	fmt.Println("移除一个订阅者")
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		return 0
	}

	count, ok := subscriber.uids[uid]
	if ok {
		if count > 1 {
			subscriber.uids[uid] = count - 1
		} else {
			delete(subscriber.uids, uid)
		}
	}
	return count
}
// 获取所有的订阅者
func (channel *Channel) GetAllSubscriber() []*AppUserID {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	subs := make([]*AppUserID, 0, 100)
	for appid, s := range channel.subscribers {
		for uid, _ := range s.uids {
			id := &AppUserID{appid: appid, uid: uid}
			subs = append(subs, id)
		}
	}
	return subs
}

func (channel *Channel) Subscribe(appid int64, uid int64) {
	fmt.Println("添加订阅者同时查询")
	count := channel.AddSubscribe(appid, uid)
	log.Info("sub count:", count)
	fmt.Println("sub count:", count)
	if count == 0 {
		id := &AppUserID{appid: appid, uid: uid}
		msg := &Message{cmd: MSG_SUBSCRIBE, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) Unsubscribe(appid int64, uid int64) {
	fmt.Println("移除订阅者")
	count := channel.RemoveSubscribe(appid, uid)
	log.Info("unsub count:", count)
	if count == 1 {
		id := &AppUserID{appid: appid, uid: uid}
		msg := &Message{cmd: MSG_UNSUBSCRIBE, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) Publish(amsg *AppMessage) {
	msg := &Message{cmd: MSG_PUBLISH, body: amsg}
	channel.wt <- msg
}

func (channel *Channel) PublishGroup(amsg *AppMessage) {
	msg := &Message{cmd: MSG_PUBLISH_GROUP, body: amsg}
	channel.wt <- msg
}

//返回添加前的计数
func (channel *Channel) AddSubscribeRoom(appid, room_id int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		subscriber = NewSubscriber()
		channel.subscribers[appid] = subscriber
	}
	//不存在count==0
	count := subscriber.room_ids[room_id]
	subscriber.room_ids[room_id] = count + 1
	return count
}

//返回删除前的计数
func (channel *Channel) RemoveSubscribeRoom(appid, room_id int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		return 0
	}

	count, ok := subscriber.room_ids[room_id]
	if ok {
		if count > 1 {
			subscriber.room_ids[room_id] = count - 1
		} else {
			delete(subscriber.room_ids, room_id)
		}
	}
	return count
}

func (channel *Channel) GetAllRoomSubscriber() []*AppRoomID {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	subs := make([]*AppRoomID, 0, 100)
	for appid, s := range channel.subscribers {
		for room_id, _ := range s.room_ids {
			id := &AppRoomID{appid: appid, room_id: room_id}
			subs = append(subs, id)
		}
	}
	return subs
}

func (channel *Channel) SubscribeRoom(appid int64, room_id int64) {
	count := channel.AddSubscribeRoom(appid, room_id)
	log.Info("sub room count:", count)
	if count == 0 {
		id := &AppRoomID{appid: appid, room_id: room_id}
		msg := &Message{cmd: MSG_SUBSCRIBE_ROOM, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) UnsubscribeRoom(appid int64, room_id int64) {
	count := channel.RemoveSubscribeRoom(appid, room_id)
	log.Info("unsub room count:", count)
	if count == 1 {
		id := &AppRoomID{appid: appid, room_id: room_id}
		msg := &Message{cmd: MSG_UNSUBSCRIBE_ROOM, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) PublishRoom(amsg *AppMessage) {
	msg := &Message{cmd: MSG_PUBLISH_ROOM, body: amsg}
	channel.wt <- msg
}

func (channel *Channel) ReSubscribe(conn *net.TCPConn, seq int) int {
	subs := channel.GetAllSubscriber()
	for _, id := range(subs) {
		msg := &Message{cmd: MSG_SUBSCRIBE, body: id}
		seq = seq + 1
		msg.seq = seq
		SendMessage(conn, msg)
	}
	return seq
}

func (channel *Channel) ReSubscribeRoom(conn *net.TCPConn, seq int) int {
	subs := channel.GetAllRoomSubscriber()
	for _, id := range(subs) {
		msg := &Message{cmd: MSG_SUBSCRIBE_ROOM, body: id}
		seq = seq + 1
		msg.seq = seq
		SendMessage(conn, msg)
	}
	return seq
}

func (channel *Channel) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	closed_ch := make(chan bool)
	seq := 0
	seq = channel.ReSubscribe(conn, seq)
	seq = channel.ReSubscribeRoom(conn, seq)

	go func() {
		for {
			msg := ReceiveMessage(conn)
			if msg == nil {
				close(closed_ch)
				return
			}
			log.Info("channel recv message:", Command(msg.cmd))
			fmt.Println("channel recv message:", Command(msg.cmd))
			fmt.Println("channel recv message:", msg)
			if msg.cmd == MSG_PUBLISH {
				fmt.Println("拿到app消息")
				amsg := msg.body.(*AppMessage)
				if channel.dispatch != nil {
					channel.dispatch(amsg)
				}
			} else if msg.cmd == MSG_PUBLISH_ROOM {
				amsg := msg.body.(*AppMessage)
				if channel.dispatch_room != nil {
					channel.dispatch_room(amsg)
				}
			} else if msg.cmd == MSG_PUBLISH_GROUP {
				amsg := msg.body.(*AppMessage)
				if channel.dispatch_group != nil {
					channel.dispatch_group(amsg)
				}
			} else {
				log.Error("unknown message cmd:", msg.cmd)
			}
		}
	}()

	for {
		select {
		case _ = <-closed_ch:
			log.Info("channel closed")
			return
		case msg := <-channel.wt:
			fmt.Println("读取通道->", msg, msg.body)
			fmt.Println("连接的地址-> ",conn.LocalAddr(), conn.RemoteAddr())
			seq = seq + 1
			msg.seq = seq
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := SendMessage(conn, msg)
			if err != nil {
				fmt.Println("channel send message:", err)
			}
		}
	}
}

func (channel *Channel) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", channel.addr)
		fmt.Println("通道连接channel addr:", channel.addr)
		if err != nil {
			log.Info("connect route server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("channel sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		fmt.Println("channel connected")
		log.Info("channel connected")
		nsleep = 100
		channel.RunOnce(tconn)
	}
}

func (channel *Channel) Start() {
	go channel.Run()
}
