
package main
import "net"
import "fmt"
import "time"
import "sync"
import "runtime"
import "flag"
import "math/rand"
import log "github.com/golang/glog"
import "os"
import "os/signal"
import "syscall"
import "github.com/garyburd/redigo/redis"
import "github.com/valyala/gorpc"

const GROUP_OFFLINE_LIMIT = 100
const GROUP_C_COUNT = 10


var storage *Storage
var config *StorageConfig
var master *Master
var group_manager *GroupManager
var clients ClientSet
var mutex   sync.Mutex
var redis_pool *redis.Pool

var group_c []chan func()

func init() {
	clients = NewClientSet()
	group_c = make([]chan func(), GROUP_C_COUNT)
	for i := 0; i < GROUP_C_COUNT; i++ {
		group_c[i] = make(chan func())
	}
}

func GetGroupChan(gid int64) chan func() {
	index := gid%GROUP_C_COUNT
	return group_c[index]
}

func GetUserChan(uid int64) chan func() {
	index := uid%GROUP_C_COUNT
	return group_c[index]
}

//clone when write, lockless when read
func AddClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()
	
	if clients.IsMember(client) {
		return
	}
	c := clients.Clone()
	c.Add(client)
	clients = c
}

func RemoveClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()

	if !clients.IsMember(client) {
		return
	}
	c := clients.Clone()
	c.Remove(client)
	clients = c
}

//group im
func FindGroupClientSet(appid int64, gid int64) ClientSet {
	s := NewClientSet()

	for c := range(clients) {
		if c.ContainAppGroupID(appid, gid) {
			s.Add(c)
		}
	}
	return s
}

func IsGroupUserOnline(appid int64, gid int64, uid int64) bool {
	for c := range(clients) {
		if c.ContainGroupUserID(appid, gid, uid) {
			return true
		}
	}
	return false
}

//peer im
func FindClientSet(id *AppUserID) ClientSet {
	s := NewClientSet()

	for c := range(clients) {
		if c.ContainAppUserID(id) {
			s.Add(c)
		}
	}
	return s
}

func IsUserOnline(appid int64, uid int64) bool {
	id := &AppUserID{appid:appid, uid:uid}
	for c := range(clients) {
		if c.ContainAppUserID(id) {
			return true
		}
	}
	return false
}


func handle_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewClient(conn)
	client.Run()
}

func Listen(f func(*net.TCPConn), listen_addr string) {
	listen, err := net.Listen("tcp", listen_addr)
	if err != nil {
		fmt.Println("初始化失败", err.Error())
		return
	}
	tcp_listener, ok := listen.(*net.TCPListener)
	if !ok {
		fmt.Println("listen error")
		return
	}

	for {
		client, err := tcp_listener.AcceptTCP()
		if err != nil {
			return
		}
		f(client)
	}
}

func ListenClient() {
	Listen(handle_client, config.listen)
}

func handle_sync_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewSyncClient(conn)
	client.Run()
}

func ListenSyncClient() {
	Listen(handle_sync_client, config.sync_listen)

}

func GroupLoop(c chan func()) {
	for {
		f := <- c
		f()
	}
}


// Signal handler
func waitSignal() error {
    ch := make(chan os.Signal, 1)
    signal.Notify(
    ch,
    syscall.SIGINT,
    syscall.SIGTERM,
    )
    for {
        sig := <-ch
        fmt.Println("singal:", sig.String())
        switch sig {
            case syscall.SIGTERM, syscall.SIGINT:
			storage.FlushReceived()
			os.Exit(0)
        }
    }
    return nil // It'll never get here.
}

func FlushLoop() {
	for {
		time.Sleep(1*time.Second)
		storage.FlushReceived()
	}
}
// 新建redis池
func NewRedisPool(server, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			timeout := time.Duration(2)*time.Second
			c, err := redis.DialTimeout("tcp", server, timeout, 0, 0)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if db > 0 && db < 16 {
				if _, err := c.Do("SELECT", db); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
}



func ListenRPCClient() {
	fmt.Println("监听rpc客户端")
	dispatcher := gorpc.NewDispatcher()
	dispatcher.AddFunc("SyncMessage", SyncMessage)
	dispatcher.AddFunc("SyncGroupMessage", SyncGroupMessage)
	dispatcher.AddFunc("SavePeerMessage", SavePeerMessage)
	dispatcher.AddFunc("SaveGroupMessage", SaveGroupMessage)
	dispatcher.AddFunc("GetNewCount", GetNewCount)

	// gorpc的服务器
	fmt.Println("rpc的地址: ", config.rpc_listen)
	s := &gorpc.Server{
		Addr: config.rpc_listen,
		Handler: dispatcher.NewHandlerFunc(),
	}

	if err := s.Serve(); err != nil {
		log.Fatalf("Cannot start rpc server: %s", err)
	}

}
func main() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: ims config")
		return
	}

	config = read_storage_cfg(flag.Args()[0])
	fmt.Printf("配置", config)
	fmt.Printf("listen:%s rpc listen:%s storage root:%s sync listen:%s master address:%s is push system:%d\n",
		config.listen, config.rpc_listen, config.storage_root, config.sync_listen, config.master_address, config.is_push_system)

	fmt.Printf("redis address:%s password:%s db:%d\n",
		config.redis_address, config.redis_password, config.redis_db)

	redis_pool = NewRedisPool(config.redis_address, config.redis_password, 
		config.redis_db)
	fmt.Println("开启了redis池:", redis_pool)
	storage = NewStorage(config.storage_root)
	
	master = NewMaster()
	fmt.Println("1")
	master.Start()
	fmt.Println("2")
	if len(config.master_address) > 0 {
		slaver := NewSlaver(config.master_address)
		slaver.Start()
	}
	fmt.Println("3")
	group_manager = NewGroupManager()
	fmt.Println("group_manager: ", group_manager)
	group_manager.Start()
	fmt.Println("4")

	for i := 0; i < GROUP_C_COUNT; i++ {
		go GroupLoop(group_c[i])
	}
	fmt.Println("5")
	//刷新storage缓存的ack
	go FlushLoop()
	fmt.Println("6")
	go waitSignal()
	fmt.Println("7")

	go ListenSyncClient()
	fmt.Println("8")
	go ListenRPCClient()

	ListenClient()
}
