package main
import "fmt"
import "encoding/json"
import log "github.com/golang/glog"


func (client *Client) IsROMApp(appid int64) bool {
	return false
}


//离线消息入apns队列
func (client *Client) PublishPeerMessage(appid int64, im *IMMessage) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = im.sender
	v["receiver"] = im.receiver
	v["content"] = im.content

	b, _ := json.Marshal(v)
	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("push_queue_%d", appid)
	} else {
		queue_name = "push_queue"
	}
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}
// 发布群消息
func (client *Client) PublishGroupMessage(appid int64, receivers []int64, im *IMMessage) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = im.sender
	v["receivers"] = receivers
	v["content"] = im.content
	v["group_id"] = im.receiver

	b, _ := json.Marshal(v)
	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("group_push_queue_%d", appid)
	} else {
		queue_name = "group_push_queue"
	}
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}
// 发布客服消息
func (client *Client) PublishCustomerMessage(appid, receiver int64, cs *CustomerMessage, cmd int) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["receiver"] = receiver
	v["command"] = cmd
	v["customer_appid"] = cs.customer_appid
	v["customer"] = cs.customer_id
	v["seller"] = cs.seller_id
	v["store"] = cs.store_id
	v["content"] = cs.content

	b, _ := json.Marshal(v)
	var queue_name string
	queue_name = "customer_push_queue"
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}

// 发布系统消息
func (client *Client) PublishSystemMessage(appid, receiver int64, content string) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["receiver"] = receiver
	v["content"] = content

	b, _ := json.Marshal(v)
	var queue_name string
	queue_name = "system_push_queue"
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}
