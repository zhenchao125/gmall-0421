package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.realtime.util.MyKafkaUtil
import com.atguigu.realtime.gmall.common.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
        val sourceStream: DStream[String] = MyKafkaUtil
            .getKafkaStream(ssc, "DauApp", Set(Constant.STARTUP_TOPIC))
        sourceStream.print()
        // 3. 各种转换 map, mapPartitions, filter, ...  transform
        
        // 3. 输出 (print foreachRdd)
        
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