package main
import "net"
import "bytes"
import "encoding/binary"
import (
	log "github.com/golang/glog"
	"fmt"
)


type Client struct {
	conn   *net.TCPConn
	
	//subscribe mode
	wt     chan *Message
	app_route *AppRoute
}
// 新建客户端
func NewClient(conn *net.TCPConn) *Client {
	client := new(Client)
	client.conn = conn 

	client.wt = make(chan *Message, 10)
	client.app_route = NewAppRoute()
	return client
}
// 是否包含该群
func (client *Client) ContainAppGroupID(appid int64, gid int64) bool {
	route := client.app_route.FindRoute(appid)
	if route == nil {
		return false
	}
	return route.ContainGroupID(gid)
}
// 包含该群组成员
func (client *Client) ContainGroupUserID(appid int64, gid int64, uid int64) bool {
	route := client.app_route.FindRoute(appid)
	if route == nil {
		return false
	}

	return route.ContainGroupMember(gid, uid)
}

func (client *Client) ContainAppUserID(id *AppUserID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.ContainUserID(id.uid)
}
// 读
func (client *Client) Read() {
	for {
		msg := client.read()
		if msg == nil {
			RemoveClient(client)
			client.wt <- nil
			break
		}
		client.HandleMessage(msg)
	}
}
// 写
func (client *Client) Write() {
	for {
		msg := <- client.wt
		if msg == nil {
			client.conn.Close()
			break
		}
		SendMessage(client.conn, msg)
	}
}


func (client *Client) HandleSaveAndEnqueueGroup(sae *SAEMessage) {
	if sae.msg == nil {
		log.Error("sae msg is nil")
		return
	}
	if sae.msg.cmd != MSG_GROUP_IM {
		log.Error("sae msg cmd:", sae.msg.cmd)
		return
	}

	appid := sae.appid
	gid := sae.receiver

	//保证群组消息以id递增的顺序发出去
	t := make(chan int64)
	f := func () {
		msgid := storage.SaveGroupMessage(appid, gid, sae.device_id, sae.msg)

		s := FindGroupClientSet(appid, gid)
		for c := range s {
			log.Info("publish group message")
			am := &AppMessage{appid:appid, receiver:gid, msgid:msgid, device_id:sae.device_id, msg:sae.msg}
			m := &Message{cmd:MSG_PUBLISH_GROUP, body:am}
			c.wt <- m
		}
		if len(s) == 0 {
			log.Infof("can't publish group message:%d", gid)
		}
		t <- msgid
	}

	c := GetGroupChan(gid)
	c <- f
	msgid := <- t

	result := &MessageResult{}
	result.status = 0
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, msgid)
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleDQGroupMessage(dq *DQGroupMessage) {
	storage.DequeueGroupOffline(dq.msgid, dq.appid, dq.gid, dq.receiver, dq.device_id)
	result := &MessageResult{status:0}
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleSaveAndEnqueue(sae *SAEMessage) {
	if sae.msg == nil {
		log.Error("sae msg is nil")
		return
	}

	appid := sae.appid
	uid := sae.receiver
	//保证消息以id递增的顺序发出
	t := make(chan int64)	
	f := func() {
		msgid := storage.SavePeerMessage(appid, uid, sae.device_id, sae.msg)
		
		id := &AppUserID{appid:appid, uid:uid}
		s := FindClientSet(id)
		for c := range s {
			am := &AppMessage{appid:appid, receiver:uid, msgid:msgid, device_id:sae.device_id, msg:sae.msg}
			m := &Message{cmd:MSG_PUBLISH, body:am}
			c.wt <- m
		}
		if len(s) == 0 {
			log.Infof("can't publish message:%s %d", Command(sae.msg.cmd), uid)
		}
		t <- msgid
	}

	c := GetUserChan(uid)
	c <- f
	msgid := <- t

	result := &MessageResult{}
	result.status = 0
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, msgid)
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}
// 读DQMessage
func (client *Client) HandleDQMessage(dq *DQMessage) {
	storage.DequeueOffline(dq.msgid, dq.appid, dq.receiver, dq.device_id)
	result := &MessageResult{status:0}
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}
// 写EMessage
func (client *Client) WriteEMessage(emsg *EMessage) []byte{
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, emsg.msgid)
	binary.Write(buffer, binary.BigEndian, emsg.device_id)
	SendMessage(buffer, emsg.msg)
	return buffer.Bytes()
}

//过滤掉自己由当前设备发出的消息
func (client *Client) filterMessages(messages []*EMessage, id *LoadOffline) []*EMessage {
	c := make([]*EMessage, 0, 10)
	
	for _, emsg := range(messages) {
		if emsg.msg.cmd == MSG_IM || 
			emsg.msg.cmd == MSG_GROUP_IM {
			m := emsg.msg.body.(*IMMessage)
			//同一台设备自己发出的消息
			if m.sender == id.uid && emsg.device_id == id.device_id {
				continue
			}
		}
		
		if emsg.msg.cmd == MSG_CUSTOMER {
			m := emsg.msg.body.(*CustomerMessage)
			if id.appid == m.customer_appid && 
				emsg.device_id == id.device_id && 
				id.uid == m.customer_id {
				continue
			}
		}

		if emsg.msg.cmd == MSG_CUSTOMER_SUPPORT {
			m := emsg.msg.body.(*CustomerMessage)
			if id.appid != m.customer_appid && 
				emsg.device_id == id.device_id && 
				id.uid == m.seller_id {
				continue
			}
		}

		c = append(c, emsg)
	}
	return c
}
// 处理加载离线消息
func (client *Client) HandleLoadOffline(id *LoadOffline) {
	messages := storage.LoadOfflineMessage(id.appid, id.uid, id.device_id)
	result := &MessageResult{status:0}
	buffer := new(bytes.Buffer)

	messages = client.filterMessages(messages, id)
	count := int16(len(messages))

	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range(messages) {
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

// 处理加载最新消息
func (client *Client) HandleLoadLatest(lh *LoadLatest) {
	messages := storage.LoadLatestMessages(lh.app_uid.appid, lh.app_uid.uid, int(lh.limit))
	result := &MessageResult{status:0}
	buffer := new(bytes.Buffer)
	var count int16
	count = int16(len(messages))
	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range(messages) {
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)	
}
// 处理加载历史消息
func (client *Client) HandleLoadHistory(lh *LoadHistory) {
	messages := storage.LoadHistoryMessages(lh.appid, lh.uid, lh.msgid)
	result := &MessageResult{status:0}
	buffer := new(bytes.Buffer)
	var count int16
	count = int16(len(messages))
	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range(messages) {
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)	
}
// 处理加载群离线消息
func (client *Client) HandleLoadGroupOffline(lh *LoadGroupOffline) {
	messages := storage.LoadGroupOfflineMessage(lh.appid, lh.gid, lh.uid, lh.device_id, GROUP_OFFLINE_LIMIT)
	result := &MessageResult{status:0}
	buffer := new(bytes.Buffer)

	var count int16 = 0
	for _, emsg := range(messages) {
		if emsg.msg.cmd == MSG_GROUP_IM {
			im := emsg.msg.body.(*IMMessage)
			if im.sender == lh.uid && emsg.device_id == lh.device_id {
				continue
			}
		}
		count += 1
	}
	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range(messages) {
		if emsg.msg.cmd == MSG_GROUP_IM {
			im := emsg.msg.body.(*IMMessage)
			if im.sender == lh.uid && emsg.device_id == lh.device_id {
				continue
			}
		}
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}
// 处理订阅群组
func (client *Client) HandleSubscribeGroup(lo *AppGroupMemberID) {
	log.Infof("subscribe group appid:%d gid:%d uid:%d\n", lo.appid, lo.gid, lo.uid)
	AddClient(client)

	route := client.app_route.FindOrAddRoute(lo.appid)
	route.AddGroupMember(lo.gid, lo.uid)
}
// 处理没有订阅的群组
func (client *Client) HandleUnSubscribeGroup(id *AppGroupMemberID) {
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveGroupMember(id.gid, id.uid)
}
// 处理订阅
func (client *Client) HandleSubscribe(id *AppUserID) {
	log.Infof("subscribe appid:%d uid:%d", id.appid, id.uid)
	AddClient(client)

	route := client.app_route.FindOrAddRoute(id.appid)
	route.AddUserID(id.uid)
}
// 处理取消订阅
func (client *Client) HandleUnsubscribe(id *AppUserID) {
	log.Infof("unsubscribe appid:%d uid:%d", id.appid, id.uid)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveUserID(id.uid)
}
// 处理初始化队列
func (client *Client) HandleInitQueue(q *InitQueue) {
	log.Infof("init queue appid:%d uid:%d device id:%d", 
		q.appid, q.uid, q.device_id)

	storage.InitQueue(q.appid, q.uid, q.device_id)
	result := &MessageResult{status:0}
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}
// 处理初始化群队列
func (client *Client) HandleInitGroupQueue(q *InitGroupQueue) {
	log.Infof("init group queue appid:%d gid:%d uid:%d device id:%d", 
		q.appid, q.gid, q.uid, q.device_id)

	storage.InitGroupQueue(q.appid, q.gid, q.uid, q.device_id)
	result := &MessageResult{status:0}
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}
// 处理消息
func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
	switch msg.cmd {
	case MSG_LOAD_OFFLINE:
		client.HandleLoadOffline(msg.body.(*LoadOffline))
	case MSG_SAVE_AND_ENQUEUE:
		client.HandleSaveAndEnqueue(msg.body.(*SAEMessage))
	case MSG_DEQUEUE:
		client.HandleDQMessage(msg.body.(*DQMessage))
	case MSG_LOAD_LATEST:
		client.HandleLoadLatest(msg.body.(*LoadLatest))
	case MSG_LOAD_HISTORY:
		client.HandleLoadHistory(msg.body.(*LoadHistory))
	case MSG_SAVE_AND_ENQUEUE_GROUP:
		client.HandleSaveAndEnqueueGroup(msg.body.(*SAEMessage))
	case MSG_DEQUEUE_GROUP:
		client.HandleDQGroupMessage(msg.body.(*DQGroupMessage))
	case MSG_SUBSCRIBE_GROUP:
		client.HandleSubscribeGroup(msg.body.(*AppGroupMemberID))
	case MSG_UNSUBSCRIBE_GROUP:
		client.HandleUnSubscribeGroup(msg.body.(*AppGroupMemberID))
	case MSG_LOAD_GROUP_OFFLINE:
		client.HandleLoadGroupOffline(msg.body.(*LoadGroupOffline))
	case MSG_SUBSCRIBE:
		client.HandleSubscribe(msg.body.(*AppUserID))
	case MSG_UNSUBSCRIBE:
		client.HandleUnsubscribe(msg.body.(*AppUserID))
	case MSG_INIT_QUEUE:
		client.HandleInitQueue(msg.body.(*InitQueue))
	case MSG_INIT_GROUP_QUEUE:
		client.HandleInitGroupQueue(msg.body.(*InitGroupQueue))
	default:
		fmt.Println("unknown msg:", msg.cmd)
		log.Warning("unknown msg:", msg.cmd)
	}
}
// 运行
func (client *Client) Run() {
	go client.Read()
	go client.Write()
}
// 读取
func (client *Client) read() *Message {
	return ReceiveMessage(client.conn)
}
// 发送
func (client *Client) send(msg *Message) {
	SendMessage(client.conn, msg)
}
// 克隆
func (client *Client) close() {
	client.conn.Close()
}
