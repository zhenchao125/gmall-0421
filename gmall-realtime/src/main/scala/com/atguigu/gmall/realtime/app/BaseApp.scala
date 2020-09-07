package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/9/7 9:26
 */
abstract class BaseApp {
    val topics: Set[String]
    val groupId: String
    val master: String
    val appName: String
    val bachTime: Int
    
    def run(sourceStream: DStream[String]): Unit
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
        val ssc = new StreamingContext(conf, Seconds(bachTime))
        
        val sourceStream: DStream[String] = MyKafkaUtil
            .getKafkaStream(ssc, groupId, topics)
        
        run(sourceStream)
        
        
        // 4. 启动上下文
        ssc.start()
        // 5. 阻塞
        ssc.awaitTermination()
    }
}
