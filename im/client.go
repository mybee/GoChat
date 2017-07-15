package main

import "net"
import "time"
import "sync/atomic"
import (
	log "github.com/golang/glog"
	"fmt"
)

type Client struct {
	Connection//必须放在结构体首部
	*IMClient
	*RoomClient
	*VOIPClient
	*CustomerClient
	public_ip int32
}

// 新建一个客户端
func NewClient(conn interface{}) *Client {
	client := new(Client)
	fmt.Println("在这里创建一个新的客户端")
	//初始化Connection
	client.conn = conn // conn is net.Conn or engineio.Conn
	fmt.Println("uid: ", client.uid)
	fmt.Println("appid: ", client.appid)
	fmt.Println("platform: ", client.platform_id)
	fmt.Println("deviceid: ", client.device_ID)

	if net_conn, ok := conn.(net.Conn); ok {
		addr := net_conn.LocalAddr()
		fmt.Println("net_conn.LocalAddr():", addr)
		if taddr, ok := addr.(*net.TCPAddr); ok {
			ip4 := taddr.IP.To4()
			fmt.Println("ip4:", ip4)
			fmt.Println("ip4的长度: ", len(ip4))
			client.public_ip = int32(ip4[0]) << 24 | int32(ip4[1]) << 16 | int32(ip4[2]) << 8 | int32(ip4[3])
			fmt.Println(client.public_ip)
		}
	}

	client.wt = make(chan *Message, 100)
	client.ewt = make(chan *EMessage, 100)
	client.owt = make(chan *EMessage, 100)

	client.unacks = make(map[int]int64)
	client.unackMessages = make(map[int]*EMessage)
	atomic.AddInt64(&server_summary.nconnections, 1)

	client.IMClient = &IMClient{&client.Connection}
	client.RoomClient = &RoomClient{Connection:&client.Connection}
	client.VOIPClient = &VOIPClient{Connection:&client.Connection}
	client.CustomerClient = NewCustomerClient(&client.Connection)
	return client
}

func (client *Client) Read() {
	for {
		tc := atomic.LoadInt32(&client.tc)
		fmt.Println("该客户端的timeout: ", tc)
		if tc > 0 {
			log.Infof("quit read goroutine, client:%d write goroutine blocked", client.uid)
			client.HandleClientClosed()
			break
		}
		fmt.Println("1")
		t1 := time.Now().Unix()
		msg := client.read()
		t2 := time.Now().Unix()
		if t2 - t1 > 6*60 {
			fmt.Println("2")
			log.Infof("client:%d socket read timeout:%d %d", client.uid, t1, t2)
		}
		fmt.Println("3")
		if msg == nil {
			fmt.Println("准备关闭客户端")
			client.HandleClientClosed()
			break
		}
		fmt.Println("4")
		client.HandleMessage(msg)
		fmt.Println("处理消息")
		t3 := time.Now().Unix()
		if t3 - t2 > 2 {
			fmt.Println("客户端处理太慢")
			log.Infof("client:%d handle message is too slow:%d %d", client.uid, t2, t3)
		}
	}
}
// 移除客户端
func (client *Client) RemoveClient() {
	route := app_route.FindRoute(client.appid)
	if route == nil {
		fmt.Println("无法找到app路由")
		log.Warning("can't find app route")
		return
	}
	route.RemoveClient(client)
	fmt.Println("移除客户端")
	if client.room_id > 0 {
		route.RemoveRoomClient(client.room_id, client)
	}
}

// 处理客户端的关闭
func (client *Client) HandleClientClosed() {
	atomic.AddInt64(&server_summary.nconnections, -1)
	if client.uid > 0 {
		atomic.AddInt64(&server_summary.nclients, -1)
	}
	atomic.StoreInt32(&client.closed, 1)

	client.RemoveClient()

	//quit when write goroutine received
	client.wt <- nil
	fmt.Println("注销客户端")
	client.RoomClient.Logout()
	client.IMClient.Logout()
}

func (client *Client) HandleMessage(msg *Message) {
	fmt.Println("消息结构体:", msg)
	fmt.Println("消息的命令: ", Command(msg.cmd))
	fmt.Println("开始处理透传消息:")
	switch msg.cmd {
	case MSG_AUTH:
		fmt.Println("处理auth消息")
		client.HandleAuth(msg.body.(*Authentication), msg.version)
	case MSG_AUTH_TOKEN:
		fmt.Println("处理auth_token消息")
		fmt.Println("token->", msg.body.(*AuthenticationToken).token)
		client.HandleAuthToken(msg.body.(*AuthenticationToken), msg.version)
	case MSG_ACK:
		fmt.Println("处理ack消息")
		client.HandleACK(msg.body.(*MessageACK))
	case MSG_HEARTBEAT:
		fmt.Println("处理心跳")
		// nothing to do
	case MSG_PING:
		fmt.Println("处理Ping")
		client.HandlePing()
	}

	client.IMClient.HandleMessage(msg)
	client.RoomClient.HandleMessage(msg)
	client.VOIPClient.HandleMessage(msg)
	client.CustomerClient.HandleMessage(msg)
}


func (client *Client) AuthToken(token string) (int64, int64, int, error) {
	fmt.Println("加载用户的token")
	appid, uid, err := LoadUserAccessToken(token)

	if err != nil {
		return 0, 0, 0, err
	}
	fmt.Println("查看用户是否被禁")
	forbidden, err := GetUserForbidden(appid, uid)
	if err != nil {
		return appid, uid, 0, nil
	} else {
		return appid, uid, forbidden, nil
	}
}

func (client *Client) HandleAuth(login *Authentication, version int)  {
	fmt.Println("处理auth")
	fmt.Println("client.uid:", client.uid)
}

func (client *Client) HandleAuthToken(login *AuthenticationToken, version int) {
	fmt.Println("处理authtoken")
	if client.uid > 0 {
		log.Info("repeat login")
		return
	}

	var err error
	appid, uid, fb, err := client.AuthToken(login.token)
	if err != nil {
		log.Infof("auth token:%s err:%s", login.token, err)
		msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{1, 0}}
		client.EnqueueMessage(msg)
		return
	}
	if  uid == 0 {
		log.Info("auth token uid==0")
		msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{1, 0}}
		client.EnqueueMessage(msg)
		return
	}

	if login.platform_id != PLATFORM_WEB && len(login.device_id) > 0{
		client.device_ID, err = GetDeviceID(login.device_id, int(login.platform_id))
		if err != nil {
			log.Info("auth token uid==0")
			msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{1, 0}}
			client.EnqueueMessage(msg)
			return
		}
	}

	client.appid = appid
	client.uid = uid
	client.forbidden = int32(fb)
	client.version = version
	client.device_id = login.device_id
	client.platform_id = login.platform_id
	client.tm = time.Now()
	log.Infof("auth token:%s appid:%d uid:%d device id:%s:%d forbidden:%d", 
		login.token, client.appid, client.uid, client.device_id, client.device_ID, client.forbidden)

	msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{0, client.public_ip}}
	client.EnqueueMessage(msg)
	// 添加到客户端列表
	client.AddClient()
	// 登录
	client.IMClient.Login()
	// 关闭
	close(client.owt)

	CountDAU(client.appid, client.uid)
	atomic.AddInt64(&server_summary.nclients, 1)
}

// 添加客户端
func (client *Client) AddClient() {
	fmt.Println("添加客户端列表")
	route := app_route.FindOrAddRoute(client.appid)
	route.AddClient(client)
}


func (client *Client) HandlePing() {
	fmt.Println("处理ping")
	m := &Message{cmd: MSG_PONG}
	client.EnqueueMessage(m)
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}
}

// 处理西消息返回
func (client *Client) HandleACK(ack *MessageACK) {
	fmt.Println("处理ack")
	log.Info("ack:", ack.seq)
}

// 给客户端发送消息
func (client *Client) Write() {
	seq := 0
	running := true
	loaded := false

	//发送离线消息
	for running && !loaded {
		select {
		case msg := <-client.wt:
			if msg == nil {
				client.close()
				running = false
				log.Infof("client:%d socket closed", client.uid)
				break
			}
			if msg.cmd == MSG_RT {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			seq++

			//以当前客户端所用版本号发送消息
			vmsg := &Message{msg.cmd, seq, client.version, msg.body}
			client.send(vmsg)
		case emsg, ok := <- client.owt:
			if !ok {
				//离线消息读取完毕
				loaded = true
				break
			}
			seq++

			emsg.msg.seq = seq

			//以当前客户端所用版本号发送消息
			msg := &Message{emsg.msg.cmd, seq, client.version, emsg.msg.body}
			if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			client.send(msg)
		}
	}
	
	//发送在线消息
	for running {
		select {
		case msg := <-client.wt:
			if msg == nil {
				client.close()
				running = false
				log.Infof("client:%d socket closed", client.uid)
				break
			}
			if msg.cmd == MSG_RT {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			seq++

			//以当前客户端所用版本号发送消息
			vmsg := &Message{msg.cmd, seq, client.version, msg.body}
			client.send(vmsg)
		case emsg := <- client.ewt:
			seq++

			emsg.msg.seq = seq

			//以当前客户端所用版本号发送消息
			msg := &Message{cmd:emsg.msg.cmd, seq:seq, version:client.version, body:emsg.msg.body}
			if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			client.send(msg)
		}
	}

	//等待200ms,避免发送者阻塞
	t := time.After(200 * time.Millisecond)
	running = true
	for running {
		select {
		case <- t:
			running = false
		case <- client.wt:
			log.Warning("msg is dropped")
		case <- client.ewt:
			log.Warning("emsg is dropped")
		}
	}

	log.Info("write goroutine exit")
}

// 处理客户端消息
func (client *Client) Run() {
	fmt.Println("客户端处理读和写")
	go client.Write()
	go client.Read()
}
