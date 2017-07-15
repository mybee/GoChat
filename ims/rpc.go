
package main

import "fmt"

func SyncMessage(addr string, sync_key *SyncHistory) []*HistoryMessage {

	if sync_key.LastMsgID == 0 {
		//兼容v1的数据
		sync_key.LastMsgID = storage.GetLastMsgID(sync_key.AppID, sync_key.Uid)
	}

	messages := storage.LoadHistoryMessages(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID)
 
	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range(messages) {
		hm := &HistoryMessage{}
		hm.MsgID = emsg.msgid
		hm.DeviceID = emsg.device_id
		hm.Cmd = int32(emsg.msg.cmd)
 
		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}
	return historyMessages
}
// 同步群消息
func SyncGroupMessage(addr string , sync_key *SyncGroupHistory) []*HistoryMessage {
	if sync_key.LastMsgID == 0 {
		sync_key.LastMsgID = storage.GetLastGroupMsgID(sync_key.AppID, sync_key.GroupID, sync_key.Uid)
	}
	messages := storage.LoadGroupHistoryMessages(sync_key.AppID, sync_key.Uid, sync_key.GroupID, sync_key.LastMsgID, GROUP_OFFLINE_LIMIT)
 
	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range(messages) {
		hm := &HistoryMessage{}
		hm.MsgID = emsg.msgid
		hm.DeviceID = emsg.device_id
		hm.Cmd = int32(emsg.msg.cmd)
 
		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}
	return historyMessages
}

// 保存单人消息
func SavePeerMessage(addr string, m *PeerMessage) (int64, error) {
	msg := &Message{cmd:int(m.Cmd), version:DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid := storage.SavePeerMessage(m.AppID, m.Uid, m.DeviceID, msg)

	//兼容版本1
	id := &AppUserID{appid:m.AppID, uid:m.Uid}
	s := FindClientSet(id)
	for c := range s {
		am := &AppMessage{appid:m.AppID, receiver:m.Uid, msgid:msgid, device_id:m.DeviceID, msg:msg}
		m := &Message{cmd:MSG_PUBLISH, body:am}
		c.wt <- m
	}
	fmt.Println("存储的消息:", m)
	fmt.Println("存储的消息的id:", msgid)

	return msgid, nil
}
// 保存群消息
func SaveGroupMessage(addr string, m *GroupMessage) (int64, error) {
	msg := &Message{cmd:int(m.Cmd), version:DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid := storage.SaveGroupMessage(m.AppID, m.GroupID, m.DeviceID, msg)

	//兼容版本1
	s := FindGroupClientSet(m.AppID, m.GroupID)
	for c := range s {
		am := &AppMessage{appid:m.AppID, receiver:m.GroupID, msgid:msgid, device_id:m.DeviceID, msg:msg}
		m := &Message{cmd:MSG_PUBLISH_GROUP, body:am}
		c.wt <- m
	}

	return msgid, nil
}

func GetNewCount(addr string, sync_key *SyncHistory) (int64, error) {
	if sync_key.LastMsgID == 0 {
		//兼容v1的数据
		sync_key.LastMsgID = storage.GetLastMsgID(sync_key.AppID, sync_key.Uid)
	}

	count := storage.GetNewCount(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID)
	return int64(count), nil
}

