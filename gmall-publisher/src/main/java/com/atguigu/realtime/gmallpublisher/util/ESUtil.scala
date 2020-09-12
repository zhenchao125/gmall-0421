package com.atguigu.realtime.gmallpublisher.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}

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
    
    def getClient: JestClient = factory.getObject
    
    
    def getDSL(date: String, keyword: String, startpage: Int, size: Int) =
        s"""
           |{
           |  "query": {
           |    "bool": {
           |      "filter": {
           |        "term": {
           |          "dt": "${date}"
           |        }
           |      },
           |      "must": [
           |        {"match": {
           |          "sku_name": {
           |            "query": "${keyword}",
           |            "operator": "and"
           |          }
           |        }}
           |      ]
           |    }
           |  },
           |  "aggs": {
           |    "group_by_user_gender": {
           |      "terms": {
           |        "field": "user_gender",
           |        "size": 2
           |      }
           |    },
           |    "group_by_user_age": {
           |      "terms": {
           |        "field": "user_age",
           |        "size": 200
           |      }
           |    }
           |  },
           |  "from": ${(startpage - 1) * size}
           |  , "size": ${size}
           |}
           |""".stripMargin
    
}

