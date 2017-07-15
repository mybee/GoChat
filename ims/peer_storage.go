
package main

import "fmt"
import "strconv"
import "strings"
import log "github.com/golang/glog"


type AppUserLoginID struct {
	appid  int64
	uid    int64
	device_id int64
}

type PeerStorage struct {
	*StorageFile
	received         map[AppUserLoginID]int64
}

func NewPeerStorage(f *StorageFile) *PeerStorage {
	storage := &PeerStorage{StorageFile:f}
	storage.received = make(map[AppUserLoginID]int64)
	return storage
}

//获取最后被接收到的消息id
func (storage *Storage) GetLastMsgID(appid int64, uid int64) int64 {
	start := fmt.Sprintf("%d_%d_", appid, uid)
	max_id := int64(0)

	//遍历"appid_uid_%did%_1",找出最大的值
	iter := storage.db.NewIterator(nil, nil)
	for ok := iter.Seek([]byte(start)); ok; ok = iter.Next() {
		k := string(iter.Key())
		v := string(iter.Value())

		if !strings.HasPrefix(k, start) {
			break
		}

		suffix := k[len(start):]
		parts := strings.Split(suffix, "_")
		if len(parts) == 2 && parts[1] == "1" {
			received_id, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				log.Error("parse int err:", err)
				break
			}

			if received_id > max_id {
				max_id = received_id
			}
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Error("iter error:", err)
	}

	log.Infof("appid:%d uid:%d max received id:%d",
		appid, uid, max_id)

	return max_id
}


func (storage *Storage) InitQueue(appid int64, uid int64, did int64) {
	id, _ := storage.GetLastReceivedID(appid, uid, did)
	//设备之前有登录
	if id > 0 {
		return
	}

	start := fmt.Sprintf("%d_%d_", appid, uid)
	max_id := int64(0)

	//遍历"appid_uid_%did%_1",找出最大的值
	iter := storage.db.NewIterator(nil, nil)
	for ok := iter.Seek([]byte(start)); ok; ok = iter.Next() {
		k := string(iter.Key())
		v := string(iter.Value())

		if !strings.HasPrefix(k, start) {
			break
		}

		suffix := k[len(start):]
		parts := strings.Split(suffix, "_")
		if len(parts) == 2 && parts[1] == "1" {
			received_id, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				log.Error("parse int err:", err)
				break
			}

			if received_id > max_id {
				max_id = received_id
			}
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Error("iter error:", err)
	}

	log.Infof("appid:%d uid:%d did:%d max received id:%d",
		appid, uid, did, max_id)

	if max_id > 0 {
		storage.mutex.Lock()
		defer storage.mutex.Unlock()

		id := AppUserLoginID{appid:appid, uid:uid, device_id:did}
		storage.received[id] = max_id
	}
}

func (storage *PeerStorage) SavePeerMessage(appid int64, uid int64, device_id int64, msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	msgid := storage.saveMessage(msg)

	last_id, _ := storage.GetLastMessageID(appid, uid)
	fmt.Println("最后的消息的id: ", last_id)
	off := &OfflineMessage{appid:appid, receiver:uid, msgid:msgid, device_id:device_id, prev_msgid:last_id}
	m := &Message{cmd:MSG_OFFLINE, body:off}
	last_id = storage.saveMessage(m)
	fmt.Println("现在最后的消息id:", last_id)
	storage.SetLastMessageID(appid, uid, last_id)
	return msgid
}



//获取最近离线消息ID
func (storage *PeerStorage) GetLastMessageID(appid int64, receiver int64) (int64, error) {
	key := fmt.Sprintf("%d_%d_0", appid, receiver)
	fmt.Println("获取时的key: ", key)
	value, err := storage.db.Get([]byte(key), nil)
	fmt.Println("获取的value : ", string(value))
	if err != nil {
		log.Error("get err:", err)
		return 0, err
	}

	msgid, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		log.Error("parseint err:", err)
		return 0, err
	}
	return msgid, nil
}

//设置最近离线消息ID
func (storage *PeerStorage) SetLastMessageID(appid int64, receiver int64, msg_id int64) {
	key := fmt.Sprintf("%d_%d_0", appid, receiver)
	value := fmt.Sprintf("%d", msg_id)
	fmt.Println("存储时的key: ", key)
	err := storage.db.Put([]byte(key), []byte(value), nil)
	fmt.Println("存储的value: ", value)
	if err != nil {
		log.Error("put err:", err)
		return
	}
}

func (storage *PeerStorage) SetLastReceivedID(appid int64, uid int64, did int64, msgid int64) {
	key := fmt.Sprintf("%d_%d_%d_1", appid, uid, did)
	value := fmt.Sprintf("%d", msgid)
	err := storage.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Error("put err:", err)
		return
	}
}

func (storage *PeerStorage) getLastReceivedID(appid int64, uid int64, did int64) (int64, error) {
	key := fmt.Sprintf("%d_%d_%d_1", appid, uid, did)
	id := AppUserLoginID{appid:appid, uid:uid, device_id:did}
	if msgid, ok := storage.received[id]; ok {
		return msgid, nil
	}

	value, err := storage.db.Get([]byte(key), nil)
	if err != nil {
		log.Error("get err:", err)
		return 0, err
	}

	msgid, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		log.Error("parseint err:", err)
		return 0, err
	}

	return msgid, nil
}

func (storage *PeerStorage) GetLastReceivedID(appid int64, uid int64, did int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.getLastReceivedID(appid, uid, did)
}


func (storage *PeerStorage) DequeueOffline(msg_id int64, appid int64, receiver int64, did int64) {
	log.Infof("dequeue offline:%d %d %d %d\n", appid, receiver, did, msg_id)
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	last, _ := storage.getLastReceivedID(appid, receiver, did)
	if msg_id <= last {
		log.Infof("ack msgid:%d last:%d\n", msg_id, last)
		return
	}
	id := AppUserLoginID{appid:appid, uid:receiver, device_id:did}
	storage.received[id] = msg_id
}

//获取所有消息id大于msgid的消息
func (storage *PeerStorage) LoadHistoryMessages(appid int64, receiver int64, msgid int64) []*EMessage {
	last_id, err := storage.GetLastMessageID(appid, receiver)
	if err != nil {
		return nil
	}
	messages := make([]*EMessage, 0, 10)
	for {
		if last_id == 0 {
			break
		}

		msg := storage.LoadMessage(last_id)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_OFFLINE {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}
		off := msg.body.(*OfflineMessage)
		if off.msgid <= msgid {
			break
		}

		msg = storage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_GROUP_IM && 
			msg.cmd != MSG_GROUP_NOTIFICATION &&
			msg.cmd != MSG_IM && 
			msg.cmd != MSG_CUSTOMER && 
			msg.cmd != MSG_CUSTOMER_SUPPORT &&
			msg.cmd != MSG_SYSTEM {
			last_id = off.prev_msgid
			continue
		}

		emsg := &EMessage{msgid:off.msgid, device_id:off.device_id, msg:msg}
		messages = append(messages, emsg)

		last_id = off.prev_msgid
	}
	return messages
}


func (storage *PeerStorage) LoadLatestMessages(appid int64, receiver int64, limit int) []*EMessage {
	last_id, err := storage.GetLastMessageID(appid, receiver)
	if err != nil {
		return nil
	}
	messages := make([]*EMessage, 0, 10)
	for {
		if last_id == 0 {
			break
		}

		msg := storage.LoadMessage(last_id)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_OFFLINE {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}
		off := msg.body.(*OfflineMessage)
		msg = storage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_GROUP_IM && 
			msg.cmd != MSG_GROUP_NOTIFICATION &&
			msg.cmd != MSG_IM && 
			msg.cmd != MSG_CUSTOMER && 
			msg.cmd != MSG_CUSTOMER_SUPPORT {
			last_id = off.prev_msgid
			continue
		}

		emsg := &EMessage{msgid:off.msgid, msg:msg}
		messages = append(messages, emsg)
		if len(messages) >= limit {
			break
		}
		last_id = off.prev_msgid
	}
	return messages
}


func (client *PeerStorage) isSender(msg *Message, appid int64, uid int64) bool {
	if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
		m := msg.body.(*IMMessage)
		if m.sender == uid {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER {
		m := msg.body.(*CustomerMessage)
		if m.customer_appid == appid && 
			m.customer_id == uid {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER_SUPPORT {
		m := msg.body.(*CustomerMessage)
		if config.kefu_appid == appid && 
			m.seller_id == uid {
			return true
		}
	}
	return false
}


func (storage *PeerStorage) GetNewCount(appid int64, uid int64, last_received_id int64) int {
	last_id, err := storage.GetLastMessageID(appid, uid)
	if err != nil {
		return 0
	}

	count := 0
	fmt.Println("last id:%d last received id:%d", last_id, last_received_id)
	log.Infof("last id:%d last received id:%d", last_id, last_received_id)

	msgid := last_id
	for ; msgid > 0; {
		msg := storage.LoadMessage(msgid)
		fmt.Println("根据消息id找到的消息:", msg)
		if msg == nil {
			fmt.Printf("load message:%d error\n", msgid)
			log.Warningf("load message:%d error\n", msgid)
			break
		}
		if msg.cmd != MSG_OFFLINE {
			fmt.Println("invalid message cmd:", Command(msg.cmd))
			log.Warning("invalid message cmd:", Command(msg.cmd))
			break
		}
		off := msg.body.(*OfflineMessage)

		if off.msgid == 0 || off.msgid <= last_received_id {
			break
		}

		msg = storage.LoadMessage(off.msgid)
		fmt.Println("msg: ", msg)
		if msg == nil {
			break
		}
		fmt.Println("msg cmd:", msg.cmd)
		if !storage.isSender(msg, appid, uid) {
			count += 1
			break
		}
		msgid = off.prev_msgid
	}

	return count
}


func (storage *PeerStorage) GetOfflineCount(appid int64, uid int64, did int64) int {
	last_id, err := storage.GetLastMessageID(appid, uid)
	if err != nil {
		return 0
	}

	count := 0
	last_received_id, _ := storage.GetLastReceivedID(appid, uid, did)

	log.Infof("last id:%d last received id:%d", last_id, last_received_id)

	msgid := last_id
	for ; msgid > 0; {
		msg := storage.LoadMessage(msgid)
		if msg == nil {
			log.Warningf("load message:%d error\n", msgid)
			break
		}
		if msg.cmd != MSG_OFFLINE {
			log.Warning("invalid message cmd:", Command(msg.cmd))
			break
		}
		off := msg.body.(*OfflineMessage)

		if off.msgid == 0 || off.msgid <= last_received_id {
			break
		}

		count += 1
		
		msgid = off.prev_msgid
	}

	return count
}


func (storage *PeerStorage) LoadOfflineMessage(appid int64, uid int64, did int64) []*EMessage {
	last_id, err := storage.GetLastMessageID(appid, uid)
	if err != nil {
		return nil
	}

	last_received_id, _ := storage.GetLastReceivedID(appid, uid, did)

	log.Infof("last id:%d last received id:%d", last_id, last_received_id)
	c := make([]*EMessage, 0, 10)
	msgid := last_id
	for ; msgid > 0; {
		msg := storage.LoadMessage(msgid)
		if msg == nil {
			log.Warningf("load message:%d error\n", msgid)
			break
		}
		if msg.cmd != MSG_OFFLINE {
			log.Warning("invalid message cmd:", Command(msg.cmd))
			break
		}
		off := msg.body.(*OfflineMessage)

		if off.msgid == 0 || off.msgid <= last_received_id {
			break
		}
		limit := 100
		//此设备首次登陆，只获取最近部分消息
		if last_received_id == 0 && len(c) >= limit {
			break
		}

		m := storage.LoadMessage(off.msgid)
		c = append(c, &EMessage{msgid:off.msgid, device_id:off.device_id, msg:m})

		msgid = off.prev_msgid
	}

	if len(c) > 0 {
		//reverse
		size := len(c)
		for i := 0; i < size/2; i++ {
			t := c[i]
			c[i] = c[size-i-1]
			c[size-i-1] = t
		}
	}

	log.Infof("load offline message appid:%d uid:%d count:%d\n", appid, uid, len(c))
	return c
}


func (storage *PeerStorage) FlushReceived() {
	if len(storage.received) > 0  {
		log.Infof("flush received:%d \n", len(storage.received))
	}

	if len(storage.received) > 0 {
		for id, msg_id := range storage.received {
			storage.SetLastReceivedID(id.appid, id.uid, id.device_id, msg_id)
			off := &MessageACKIn{appid:id.appid, receiver:id.uid, msgid:msg_id, device_id:id.device_id}
			msg := &Message{cmd:MSG_ACK_IN, body:off}
			storage.saveMessage(msg)
		}
		storage.received = make(map[AppUserLoginID]int64)
	}
}

func (storage *PeerStorage) ExecMessage(msg *Message, msgid int64) {
	if msg.cmd == MSG_OFFLINE {
		off := msg.body.(*OfflineMessage)
		storage.SetLastMessageID(off.appid, off.receiver, msgid)
	} else if msg.cmd == MSG_ACK_IN {
		off := msg.body.(*MessageACKIn)
		did := off.device_id
		storage.SetLastReceivedID(off.appid, off.receiver, did, off.msgid)
	}
}
