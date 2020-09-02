#!/bin/bash
# 群起nginx和日志服务器   停止
log_home=/opt/realtime0421
case $1 in
"start")
    # 启动nginx
    echo "在hadoop102启动nginx"
    sudo /usr/local/webserver/nginx/sbin/nginx
    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "在$host 启动日志服务器"
        ssh $host "source /etc/profile; nohup java -jar $log_home/gmall-log-0.0.1-SNAPSHOT.jar >/opt/realtime0421/log.log 2>&1  &"
    done

;;
"stop")
    echo "在hadoop102停止nginx"
    sudo /usr/local/webserver/nginx/sbin/nginx -s stop
    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "在$host 停止日志服务器"
        ssh $host "source /etc/profile; jps | awk ' /gmall-log-0.0.1-SNAPSHOT.jar/ {print \$1}' | xargs kill -9"
    done
;;
*)
   echo "你启动的姿势不对"
   echo "   log.sh start 启动nginx和日志服务器"
   echo "   log.sh stop  停止nginx和日志服务器"
esac


