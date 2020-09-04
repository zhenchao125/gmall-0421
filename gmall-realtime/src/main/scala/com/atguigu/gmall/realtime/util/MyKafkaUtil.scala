package com.atguigu.gmall.realtime.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable

/**
 * Author atguigu
 * Date 2020/9/4 9:29
 */
object MyKafkaUtil {
    val kafkaParams = mutable.Map[String, Object](
        "bootstrap.servers" -> ConfigUtil.getConf("kafka.servers"),
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "latest", // 如果能读到上次消费的位置, 就从这个位置开始消费, 如果没有, 则从最新
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    
    def getKafkaStream(ssc: StreamingContext, groupId: String, topics: Set[String]) = {
        kafkaParams("group.id") = groupId
        
        KafkaUtils
            .createDirectStream[String, String](
                ssc,
                PreferConsistent,
                Subscribe[String, String](topics, kafkaParams)
            )
            .map(_.value())
    }
}
