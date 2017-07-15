package main

import log "github.com/golang/glog"


type VOIPClient struct {
	*Connection
}

func (client *VOIPClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_VOIP_CONTROL:
		client.HandleVOIPControl(msg.body.(*VOIPControl))
	}
}

func (client *VOIPClient) HandleVOIPControl(msg *VOIPControl) {
	log.Info("send voip control:", msg.receiver)
	m := &Message{cmd: MSG_VOIP_CONTROL, body: msg}
	client.SendMessage(msg.receiver, m)
}

