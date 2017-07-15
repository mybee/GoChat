package main

import "fmt"
import "time"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"
import "errors"

func GetSyncKey(appid int64, uid int64) int64 {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	origin, err := redis.Int64(conn.Do("HGET", key, "sync_key"))
	if err != nil && err != redis.ErrNil {
		log.Info("hget error:", err)
		return 0
	}
	return origin
}

func GetGroupSyncKey(appid int64, uid int64, group_id int64) int64 {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	field := fmt.Sprintf("group_sync_key_%d", group_id)

	origin, err := redis.Int64(conn.Do("HGET", key, field))
	if err != nil && err != redis.ErrNil {
		log.Info("hget error:", err)
		return 0
	}
	return origin
}

func SaveSyncKey(appid int64, uid int64, sync_key int64) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	_, err := conn.Do("HSET", key, "sync_key", sync_key)
	if err != nil {
		log.Warning("hset error:", err)
	}
}

func SaveGroupSyncKey(appid int64, uid int64, group_id int64, sync_key int64) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	field := fmt.Sprintf("group_sync_key_%d", group_id)

	_, err := conn.Do("HSET", key, field, sync_key)
	if err != nil {
		log.Warning("hset error:", err)
	}	
}


func GetUserForbidden(appid int64, uid int64) (int, error) {
	conn := redis_pool.Get()
	defer conn.Close()


	key := fmt.Sprintf("users_%d_%d", appid, uid)

	// 设置forbidden(TODO)
	_, err := conn.Do("HSET", key, "forbidden", 0)
	if err != nil {
		fmt.Println("设置forbidden时出错")
	}

	forbidden, err := redis.Int(conn.Do("HGET", key, "forbidden"))
	if err != nil {
		log.Info("hget error:", err)
		return 0,  err
	}

	return forbidden, nil
}

func LoadUserAccessToken(key string) (int64, int64, error) {
	conn := redis_pool.Get()
	fmt.Println("拿到redis了连接池")
	defer conn.Close()
	//_, err := conn.Do("HMSET", "access_token_123456", "user_id", "123", "app_id", "123")
	//if err != nil {
	//	fmt.Println("hmset 的时候出现错误:", err)
	//}
	// 拼接token的key
	//key := fmt.Sprintf("access_token_%s", token)
	var uid int64
	var appid int64

	// 判断该token是否存在
	// 拼接token的key
	fmt.Println("要查找的key->", key)
	exists, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		fmt.Println("判断该token是否存在的时候出现错误")
		return 0, 0, err
	}
	if !exists {
		fmt.Println("token 不存在!")
		return 0, 0,  errors.New("token non exists")
	}

	// 获取appid和userid
	reply, err := redis.Values(conn.Do("HMGET", key, "user_id", "app_id"))
	if err != nil {
		fmt.Println("获取hmget userid appid出错")
		log.Info("hmget error:", err)
		return 0, 0, err
	}

	_, err = redis.Scan(reply, &uid, &appid)
	if err != nil {
		fmt.Println("scan error:", err)
		log.Warning("scan error:", err)
		return 0, 0, err
	}
	return appid, uid, nil	
}

func CountUser(appid int64, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("statistics_users_%d", appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func CountDAU(appid int64, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	now := time.Now()
	date := fmt.Sprintf("%d_%d_%d", now.Year(), int(now.Month()), now.Day())
	key := fmt.Sprintf("statistics_dau_%s_%d", date, appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func SetUserUnreadCount(appid int64, uid int64, count int32) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	_, err := conn.Do("HSET", key, "unread", count)
	if err != nil {
		log.Info("hset err:", err)
	}
}
