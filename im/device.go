package main
import "fmt"
import "github.com/garyburd/redigo/redis"

func GetDeviceID(device_id string, platform_id int) (int64, error) {
	conn := redis_pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("devices_%s_%d", device_id, platform_id)
	device_ID, err := redis.Int64(conn.Do("GET", key))
	if err != nil {
		k := "devices_id"
		device_ID, err = redis.Int64(conn.Do("INCR", k))
		if err != nil {
			return 0, err
		}
		_, err = conn.Do("SET", key, device_ID)
		if err != nil {
			return 0, err
		}
	}
	return device_ID, err
}
