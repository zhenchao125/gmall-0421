package com.atguigu.gmall.realtime.util

import redis.clients.jedis.Jedis

object RedisUtil {
    val host = ConfigUtil.getConf("redis.server")
    val port = ConfigUtil.getConf("redis.port").toInt
    def getClient = {
        new Jedis(host, port)
    }
}
