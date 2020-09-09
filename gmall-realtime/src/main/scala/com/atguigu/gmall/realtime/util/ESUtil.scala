package com.atguigu.gmall.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.Index

import scala.collection.JavaConverters._

/**
 * Author atguigu
 * Date 2020/9/9 10:20
 */
object ESUtil {
    val esUrls = Set("http://hadoop102:9200", "http://hadoop103:9200", "http://hadoop104:9200").asJava
    val factory = new JestClientFactory
    val conf = new HttpClientConfig.Builder(esUrls)
        .maxTotalConnection(100)
        .connTimeout(1000 * 10)
        .readTimeout(1000 * 10)
        .build()
    factory.setHttpClientConfig(conf)
    
    def main(args: Array[String]): Unit = {
        
        
        /*val source =
            """
              |{
              |  "name": "lisi",
              |  "age": 20
              |}
              |""".stripMargin*/
        val source = User("aaaaa", 20)
        
//        insertSingle("user0421", ("100", source))
//        insertSingle("user0421", (100, source))
//        insertSingle("user0421", source)
    }
    
    
    def insertSingle(index: String, source: Object): Unit = {
        val client: JestClient = factory.getObject
        
        val action = source match {
            case (id: String, s) =>
                new Index.Builder(s)
                    .index(index)
                    .`type`("_doc")
                    .id(id)
                    .build()
            case s =>
                new Index.Builder(s)
                    .index(index)
                    .`type`("_doc")
                    .build()
        }
        
        client.execute(action)
        client.close()
    }
    
    
}

case class User(name: String, age: Int)

/*
POST /user0421/_doc/1
{
  "name": "lisi",
  "age": 20
}


 */
