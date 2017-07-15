package main

import "strconv"
import "log"
import "github.com/richmonkey/cfg"

type RouteConfig struct {
	listen string
	mysqldb_datasource  string
	redis_address       string
	redis_password      string
	redis_db            int
	is_push_system      bool
}

func get_int(app_cfg map[string]string, key string) int {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	n, err := strconv.ParseInt(concurrency, 10, 64)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return int(n)
}

func get_opt_int(app_cfg map[string]string, key string) int64 {
	concurrency, present := app_cfg[key]
	if !present {
		return 0
	}
	n, err := strconv.ParseInt(concurrency, 10, 64)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return n
}


func get_string(app_cfg map[string]string, key string) string {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	return concurrency
}

func get_opt_string(app_cfg map[string]string, key string) string {
	concurrency, present := app_cfg[key]
	if !present {
		return ""
	}
	return concurrency
}

func read_route_cfg(cfg_path string) *RouteConfig {
	config := new(RouteConfig)
	app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

	config.listen = get_string(app_cfg, "listen")
	config.mysqldb_datasource = get_string(app_cfg, "mysqldb_source")
	config.redis_address = get_string(app_cfg, "redis_address")
	config.redis_password = get_opt_string(app_cfg, "redis_password")
	db := get_opt_int(app_cfg, "redis_db")
	config.redis_db = int(db)
	config.is_push_system = get_opt_int(app_cfg, "is_push_system") == 1

	return config
}
