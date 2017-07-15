package main

import "io"
import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"
import (
	"errors"
	"fmt"
)

//平台号
const PLATFORM_IOS = 1
const PLATFORM_ANDROID = 2
const PLATFORM_WEB = 3

const DEFAULT_VERSION = 1

var message_descriptions map[int]string = make(map[int]string)

type MessageCreator func()IMessage
var message_creators map[int]MessageCreator = make(map[int]MessageCreator)

type VersionMessageCreator func()IVersionMessage
var vmessage_creators map[int]VersionMessageCreator = make(map[int]VersionMessageCreator)


// 写头部
func WriteHeader(len int32, seq int32, cmd byte, version byte, buffer io.Writer) {
	binary.Write(buffer, binary.BigEndian, len)
	binary.Write(buffer, binary.BigEndian, seq)
	t := []byte{cmd, byte(version), 0, 0}
	buffer.Write(t)
}
// 读取头部
func ReadHeader(buff []byte) (int, int, int, int) {
	var length int32
	var seq int32
	buffer := bytes.NewBuffer(buff)
	fmt.Println("buffer: ", buffer)
	binary.Read(buffer, binary.BigEndian, &length)
	binary.Read(buffer, binary.BigEndian, &seq)
	cmd, _ := buffer.ReadByte()
	version, _ := buffer.ReadByte()
	fmt.Println("length: ", int(length))
	fmt.Println("seq: ", int(seq))
	fmt.Println("cmd: ", int(cmd))
	fmt.Println("version: ", int(version))
	return int(length), int(seq), int(cmd), int(version)
}
// 写消息
func WriteMessage(w *bytes.Buffer, msg *Message) {
	body := msg.ToData()
	WriteHeader(int32(len(body)), int32(msg.seq), byte(msg.cmd), byte(msg.version), w)
	w.Write(body)
}
// 发送消息
func SendMessage(conn io.Writer, msg *Message) error {
	buffer := new(bytes.Buffer)
	WriteMessage(buffer, msg)
	buf := buffer.Bytes()
	fmt.Println("要发送的buf:", buf)
	n, err := conn.Write(buf)
	if err != nil {
		fmt.Println("sock write error:", err)
		log.Info("sock write error:", err)
		return err
	}
	if n != len(buf) {
		fmt.Printf("write less:%d %d", n, len(buf))
		log.Infof("write less:%d %d", n, len(buf))
		return errors.New("write less")
	}
	return nil
}
// 接收限定消息
func ReceiveLimitMessage(conn io.Reader, limit_size int) *Message {
	buff := make([]byte, 12)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	length, seq, cmd, version := ReadHeader(buff)
	if length < 0 || length >= limit_size {
		fmt.Println("invalid len:", length)
		return nil
	}
	buff = make([]byte, length)
	_, err = io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		fmt.Println("sock read error:", err)
		return nil
	}

	message := new(Message)
	message.cmd = cmd
	message.seq = seq
	message.version = version
	if !message.FromData(buff) {
		log.Warning("parse error")
		fmt.Println("parse error")
		return nil
	}
	fmt.Println("收到的消息:", message.body)
	return message
}

// 接收消息
func ReceiveMessage(conn io.Reader) *Message {
	fmt.Println("读取正常的连接")
	return ReceiveLimitMessage(conn, 32*1024)
}

//消息大小限制在1M
func ReceiveStorageMessage(conn io.Reader) *Message {
	return ReceiveLimitMessage(conn, 1024*1024)
}


