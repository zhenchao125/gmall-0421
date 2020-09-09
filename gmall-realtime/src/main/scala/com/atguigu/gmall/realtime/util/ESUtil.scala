package com.atguigu.gmall.realtime.util

import com.atguigu.gmall.realtime.bean.AlertInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD

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
    
    def insertBulk(index: String, sources: Iterator[Object]): Unit = {
        val client: JestClient = factory.getObject
        val builder = new Bulk.Builder()
            .defaultIndex(index)
            .defaultType("_doc")
        /*sources.foreach(source => {
            val action = source match {
                case (id: String, s) =>
                    new Index.Builder(s)
                        .id(id)
                        .build()
                case s =>
                    new Index.Builder(s)
                        .build()
            }
            builder.addAction(action)
        })*/
        
        /*sources.foreach {
            case (id: String, s) =>
                val action = new Index.Builder(s)
                    .id(id)
                    .build()
                builder.addAction(action)
            case s =>
                val action = new Index.Builder(s)
                    .build()
                builder.addAction(action)
        }*/
        sources
            .map {
                case (id: String, s) =>
                    new Index.Builder(s)
                        .id(id)
                        .build()
                
                case s =>
                    new Index.Builder(s)
                        .build()
                
            }
            .foreach(builder.addAction)
        
        client.execute(builder.build())
        client.close()
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
    
    implicit class RichES(rdd: RDD[AlertInfo]) {
        def saveToES(index: String): Unit = {
            rdd.foreachPartition(infoIt => {
                ESUtil.insertBulk(index, infoIt.map(info => (s"${info.mid}_${info.ts / 1000 / 60}", info)))
            })
        }
    }
}

