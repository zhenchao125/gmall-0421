package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.realtime.gmall.common.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/9/4 9:17
 */
object DauApp {
    def main(args: Array[String]): Unit = {
        // 1.先创建 StreamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 2. 从kafka得到一个stream
        val sourceStream: DStream[StartupLog] = MyKafkaUtil
            .getKafkaStream(ssc, "DauApp", Set(Constant.STARTUP_TOPIC))
            .map(json => {
                JSON.parseObject(json, classOf[StartupLog])
            })
        // 3. 各种转换 map, mapPartitions, filter, ...  transform
        // 3.1 只保留每个设备每天第一次启动记录
        /*val filteredStartupLogStream =  sourceStream.filter(startupLog => {
             // 把启动日志的mid_id写入到redis, 如果返回值是1表示是第一次, 不是1就不是第一次
             println("过滤前: " + startupLog)
             val client: Jedis = RedisUtil.getClient
             val r = client.sadd(s"dau:uids:${startupLog.logDate}", startupLog.mid)
             client.close()
             r == 1
         })*/
        
        // 和外界的沟通的时候, 需要客户端, 要一个分区创建一个客户端
        val filteredStartupLogStream = sourceStream.mapPartitions((startupLogIt: Iterator[StartupLog]) => {
            val client: Jedis = RedisUtil.getClient
            val result = startupLogIt.filter(startupLog => {
                println("过滤前: " + startupLog)
                client.sadd(s"dau:uids:${startupLog.logDate}", startupLog.mid) == 1
            })
            client.close()
            result
        })
        /* // 代码1: driver  只会执行一次
         sourceStream.transform(rdd => {
             // 代码2: driver  每个批次执行一次
             rdd.mapPartitions((startupLogIt: Iterator[StartupLog]) => {
                 // 代码3:executor
                 val client: Jedis = RedisUtil.getClient
                 val result = startupLogIt.filter(startupLog => {
                     println("过滤前: " + startupLog)
                     client.sadd(s"dau:uids:${startupLog.logDate}", startupLog.mid) == 1
                 })
                 client.close()
                 result
             })
         })*/
        // 3. 输出 (print foreachRdd)
        filteredStartupLogStream.foreachRDD(rdd => {
            import org.apache.phoenix.spark._
            rdd.saveToPhoenix("GMALL_DAU0421",
                Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
                zkUrl = Option("hadoop102,hadoop103,hadoop104:2181")
            )
        })
        // 4. 启动上下文
        ssc.start()
        // 5. 阻塞
        ssc.awaitTermination()
    }
}

/*
kafka分区数如何设定?
    根据spark集群的能力

 */