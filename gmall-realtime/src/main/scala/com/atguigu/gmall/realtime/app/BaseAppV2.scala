package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/9/7 9:26
 */
abstract class BaseAppV2 {
    val topics: Set[String]
    val groupId: String
    val master: String
    val appName: String
    val bachTime: Int
    
    def run(streams: Map[String, DStream[String]]): Unit
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
        val ssc = new StreamingContext(conf, Seconds(bachTime))
        
       val streams =  topics.map(topic => {
           (topic, MyKafkaUtil.getKafkaStream(ssc, groupId, Set(topic)))
        }).toMap
        
        run(streams)
        
        
        // 4. 启动上下文
        ssc.start()
        // 5. 阻塞
        ssc.awaitTermination()
    }
}
