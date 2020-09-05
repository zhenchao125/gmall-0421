package com.atguigu.gmall.canal

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * Author atguigu
 * Date 2020/9/5 15:52
 */
object MyKafkaUtil {
    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    // key序列化
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // value序列化
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    val producer = new KafkaProducer[String, String](props)
    
    def send(topic: String, content: String): Unit = {
        producer.send(new ProducerRecord[String, String](topic, content))
    }
}
