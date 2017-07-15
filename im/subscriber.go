package main
import "time"
import "strings"
import "strconv"
import "sync/atomic"
import "github.com/garyburd/redigo/redis"
import (
	log "github.com/golang/glog"
	"fmt"
)


func HandleForbidden(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 3 {
		log.Info("message error:", data)
		return
	}
	appid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	fb, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	route := app_route.FindRoute(appid)
	if route == nil {
		log.Warningf("can't find appid:%d route", appid)
		return
	}
	clients := route.FindClientSet(uid)
	if len(clients) == 0 {
		return
	}

	log.Infof("forbidden:%d %d %d client count:%d", 
		appid, uid, fb, len(clients))
	for c, _ := range(clients) {
		atomic.StoreInt32(&c.forbidden, int32(fb))
	}
}

func SubscribeRedis() bool {
	c, err := redis.Dial("tcp", config.redis_address)
	if err != nil {
		log.Info("dial redis error:", err)
		return false
	}

	password := config.redis_password
	if len(password) > 0 {
		fmt.Println("验证redis的密码")
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return false
		}
	}

	psc := redis.PubSubConn{c}
	psc.Subscribe("speak_forbidden")

	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			log.Infof("%s: message: %s\n", v.Channel, v.Data)
			if v.Channel == "speak_forbidden" {
				HandleForbidden(string(v.Data))
			}
		case redis.Subscription:
			log.Infof("%s: %s %d\n", v.Channel, v.Kind, v.Count)
		case error:
			log.Info("error:", v)
			return true
		}
	}
}

func ListenRedis() {
	nsleep := 1
	for {
		connected := SubscribeRedis()
		if !connected {
			nsleep *= 2
			if nsleep > 60 {
				nsleep = 60
			}
		} else {
			nsleep = 1
		}
		time.Sleep(time.Duration(nsleep) * time.Second)
	}
}
