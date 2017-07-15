package main

import (
	log "github.com/golang/glog"
	"github.com/googollee/go-engine.io"
	"io/ioutil"
	"net/http"
	"bytes"
	"fmt"
)

type SIOServer struct {
	server *engineio.Server
}

func (s *SIOServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Info(req.Header.Get("Origin"))
	if req.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", req.Header.Get("Origin"))
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", `Origin, No-Cache, X-Requested-With, If-Modified-Since, Pragma,
		Last-Modified, Cache-Control, Expires, Content-Type`)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	} else {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	}
	s.server.ServeHTTP(w, req)
}

// 启动socket
func StartSocketIO(address string, tls_address string, 
	cert_file string, key_file string) {
	server, err := engineio.NewServer(nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			conn, err := server.Accept()
			if err != nil {
				fmt.Println("连接失败")
				log.Info("accept connect fail")
			}
			fmt.Println("连接成功")
			handlerEngineIOClient(conn)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/engine.io/", &SIOServer{server})
	log.Infof("EngineIO Serving at %s...", address)

	if tls_address != "" && cert_file != "" && key_file != "" {
		go func() {
			log.Infof("EngineIO Serving TLS at %s...", tls_address)
			err = http.ListenAndServeTLS(tls_address, cert_file, key_file, mux)
			if err != nil {
				log.Fatalf("listen err:%s", err)
			}
		}()
	}
	err = http.ListenAndServe(address, mux)
	if err != nil {
		log.Fatalf("listen err:%s", err)
	}
}

func handlerEngineIOClient(conn engineio.Conn) {
	client := NewClient(conn)
	fmt.Println("有一个客户端连了进来")
	client.Run()
}

func SendEngineIOBinaryMessage(conn engineio.Conn, msg *Message) {
	w, err := conn.NextWriter(engineio.MessageBinary)
	if err != nil {
		log.Info("get next writer fail")
		return
	}
	log.Info("message version:", msg.version)
	err = SendMessage(w, msg)
	if err != nil {
		log.Info("engine io write error")
		return
	}
	w.Close()
}

func ReadEngineIOMessage(conn engineio.Conn) *Message {
	t, r, err := conn.NextReader()
	fmt.Printf("读取engine.io的内容")
	if err != nil {
		return nil
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil
	}
	r.Close()
	if t == engineio.MessageText {
		fmt.Println("是文本消息")
		return nil
	} else {
		fmt.Println("是二进制消息")
		return ReadBinaryMesage(b)
	}
}

func ReadBinaryMesage(b []byte) *Message {
	reader := bytes.NewReader(b)
	return ReceiveMessage(reader)
}

