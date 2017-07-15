package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"encoding/base64"
	"encoding/json"
	"crypto/md5"
)
import (
	"github.com/bitly/go-simplejson"
	"time"
	"math/rand"
	"net"
	"log"
	"flag"
	"strconv"
)

var first int64
var local_ip string
var host string
var port int

const HOST = "127.0.0.1"
const PORT = 23000

const APP_ID = 7
const APP_KEY = "sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44"
const APP_SECRET = "0WiCxAU1jh76SbgaaFC7qIaBPm2zkyM1"
const URL = "http://192.168.2.110:6666"

func init() {
	flag.Int64Var(&first, "first", 0, "first uid")
	flag.StringVar(&local_ip, "local_ip", "0.0.0.0", "local ip")
	flag.StringVar(&host, "host", "127.0.0.1", "host")
	flag.IntVar(&port, "port", 23000, "port")
}

func main()  {

	receive_loop(456)

}

// 登录
func login(uid int64) string {
	url := URL + "/auth/grant"
	secret := fmt.Sprintf("%x", md5.Sum([]byte(APP_SECRET)))
	s := fmt.Sprintf("%d:%s", APP_ID, secret)
	fmt.Println("appid和加密后的appsecret: ", s)
	basic := base64.StdEncoding.EncodeToString([]byte(s))
	fmt.Println("basic: ", basic)

	v := make(map[string]interface{})
	v["uid"] = uid
	fmt.Println("uid: ", uid)
	str := fmt.Sprintf("uid=%d", uid)
	body, _ := json.Marshal(v)

	client := &http.Client{}
	fmt.Println("body: ", string(body))
	fmt.Println("url: ", fmt.Sprintf("%s?uid=%d", url,uid))
	fmt.Println("str: ", str)
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s?uid=%d", url,uid), strings.NewReader(str))

	req.Header.Set("Authorization", "Basic " + basic)
	fmt.Println("auth:", basic)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println("请求错误: ", err)
		return ""
	}
	defer res.Body.Close()
	fmt.Println("response body : ", res.Body)

	b, err := ioutil.ReadAll(res.Body)
	fmt.Println("response body : ", string(b))

	if err != nil {
		return ""
	}
	// 转化为json
	// 转化为json
	obj, err := simplejson.NewJson(b)
	fmt.Println("obj :", obj)
	token, _ := obj.Get("token").Int()
	fmt.Println("json--token-->", token)
	token_string := strconv.Itoa(token)
	return string(token_string)
}


func receive(uid int64, token string) {
	ip := net.ParseIP(host)
	addr := net.TCPAddr{ip, port, ""}

	lip := net.ParseIP(local_ip)
	laddr := net.TCPAddr{lip, 0, ""}

	conn, err := net.DialTCP("tcp4", &laddr, &addr)
	fmt.Println("conn laddr, raddr", conn.LocalAddr(), conn.RemoteAddr())
	if err != nil {
		log.Println("connect error")
		return
	}

	seq := 1
	//SendMessage(conn, &Message{MSG_AUTH, seq, DEFAULT_VERSION, &Authentication{uid: uid}})
	auth := &AuthenticationToken{token:token, platform_id:1, device_id:"00000000"}
	SendMessage(conn, &Message{MSG_AUTH_TOKEN, seq, DEFAULT_VERSION, auth})
	ReceiveMessage(conn)

	q := make(chan bool, 10)
	wt := make(chan *Message, 10)

	const HEARTBEAT_TIMEOUT = 3 * 60
	go func() {
		msgid := 0
		ticker := time.NewTicker(HEARTBEAT_TIMEOUT * time.Second)
		ticker2 := time.NewTicker(1000 * time.Second)
		for {
			select {
			case <-ticker2.C:
				seq++
				msgid++
				receiver := 123
				im := &IMMessage{uid, int64(receiver), 0, int32(msgid), "hello"}
				SendMessage(conn, &Message{MSG_IM, seq, DEFAULT_VERSION, im})
				fmt.Println("发送了一条im消息")
			case <-ticker.C:
				seq++
				SendMessage(conn, &Message{MSG_PING, seq, DEFAULT_VERSION, nil})
				fmt.Println("发送了一条ping消息")
			case m := <-wt:
				if m == nil {
					q <- true
					return
				}
				seq++
				m.seq = seq
				SendMessage(conn, m)
			}
		}
	}()

	go func() {
		for {
			msg := ReceiveMessage(conn)
			fmt.Println("我在等待接收消息", msg)
			//b, err := Encode(msg)
			//if err != nil {
			//	//错误处理
			//}
			//fmt.Println("我在等待接收消息", b)
			if msg == nil {
				wt <- nil
				q <- true
				return
			}

			if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
				fmt.Println("消息-->", msg.body.(*IMMessage).content)
				ack := &Message{cmd: MSG_ACK, body: &MessageACK{int32(msg.seq)}}
				wt <- ack
			}
		}
	}()

	<-q
	<-q
	conn.Close()
}

func receive_loop(uid int64) {

	receive(uid, login(uid))
	n := rand.Int()
	n = n % 20
	time.Sleep(time.Duration(n) * time.Second)

}

//func Encode(data interface{}) ([]byte, error) {
//	buf := bytes.NewBuffer(nil)
//	enc := gob.NewEncoder(buf)
//	err := enc.Encode(data)
//	if err != nil {
//		return nil, err
//	}
//	return buf.Bytes(), nil
//}
