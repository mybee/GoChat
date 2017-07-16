
# GoChat 
 ![image](https://github.com/mybee/GoChat/blob/master/img/Snip20170716_43.png)
1. 支持点对点消息, 群组消息
2. 支持集群部署

## 编译运行

  make install

  可执行程序在bin目录下

3. 安装mysql数据库, redis, 并导入db.sql

4. 配置程序
   配置项的说明参考ims.cfg.sample, imr.cfg.sample, im.cfg.sample


5. 启动程序

    nohup $BASEDIR/ims -log_dir=/data/logs/ims ims.cfg >/data/logs/ims/ims.log 2>&1 &

    nohup $BASEDIR/imr -log_dir=/data/logs/imr imr.cfg >/data/logs/imr/imr.log 2>&1 &

    nohup $BASEDIR/im -log_dir=/data/logs/im im.cfg >/data/logs/im/im.log 2>&1 &

6. 分布式部署
    
    --> TODO