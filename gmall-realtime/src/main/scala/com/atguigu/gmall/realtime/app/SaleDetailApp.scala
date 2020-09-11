package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall.realtime.util.RedisUtil
import com.atguigu.realtime.gmall.common.Constant
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._

/**
 * Author atguigu
 * Date 2020/9/11 9:05
 */
object SaleDetailApp extends BaseAppV2 {
    override val topics: Set[String] = Set(Constant.ORDER_INFO_TOPIC, Constant.ORDER_DETAIL_TOPIC)
    override val groupId: String = "SaleDetailApp1"
    override val master: String = "local[2]"
    override val appName: String = "SaleDetailApp"
    override val bachTime: Int = 3
    
    // 把orderInfo的数据缓存到redis中
    def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo) = {
        implicit val f = org.json4s.DefaultFormats
        //        client.set("order_info:" + orderInfo.id, Serialization.write(orderInfo))
        client.setex("order_info:" + orderInfo.id, 60 * 30, Serialization.write(orderInfo))
    }
    
    // 缓存OrderDetail
    def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail) = {
        implicit val f = org.json4s.DefaultFormats
        client.hset("order_detail:" + orderDetail.order_id, orderDetail.id, Serialization.write(orderDetail))
        client.expire("order_detail:" + orderDetail.order_id, 60 * 30)
    }
    
    def joinOrderInfoOrderDetail(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
        val orderIdToOrderInfoStream = orderInfoStream
            .map(info => (info.id, info))
        val orderIdToOrderDetailStream = orderDetailStream
            .map(detail => (detail.order_id, detail))
        
        // 必须使用fullJoin
        orderIdToOrderInfoStream
            .fullOuterJoin(orderIdToOrderDetailStream)
            .mapPartitions(it => {
                val client: Jedis = RedisUtil.getClient
                val r = it.flatMap {
                    // some some
                    case (_, (Some(orderInfo), Some(orderDetail))) =>
                        println("some  some")
                        // 1. 把orderInfo写缓存
                        cacheOrderInfo(client, orderInfo)
                        // 2. 合并成一个SaleDetail
                        val saleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        // 3. 去orderDetail的缓存中读取数据
                        if (client.exists("order_detail:" + orderInfo.id)) {
                            val t = client
                                .hgetAll("order_detail:" + orderInfo.id)
                                .asScala
                                .map {
                                    case (_, json) =>
                                        val orderDetail = JSON.parseObject(json, classOf[OrderDetail])
                                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                                }
                                .toList :+ saleDetail
                            client.del("order_detail:" + orderInfo.id)
                            t
                        } else {
                            // flatMap要求返回值必须是一个集合
                            saleDetail :: Nil
                        }
                    // none some
                    case (orderId, (Some(orderInfo), None)) =>
                        println("some  none")
                        cacheOrderInfo(client, orderInfo)
                        if (client.exists("order_detail:" + orderInfo.id)) {
                            val t = client
                                .hgetAll("order_detail:" + orderInfo.id)
                                .asScala
                                .map {
                                    case (_, json) =>
                                        val orderDetail = JSON.parseObject(json, classOf[OrderDetail])
                                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                                }
                                .toList
                            client.del("order_detail:" + orderInfo.id)
                            t
                        } else {
                            // flatMap要求返回值必须是一个集合
                            Nil
                        }
                    // some none
                    case (orderId, (None, Some(orderDetail))) =>
                        println("none  some")
                        if (client.exists("order_info:" + orderDetail.order_id)) {
                            // order_id存在
                            val json: String = client.get("order_info:" + orderDetail.order_id)
                            val orderInfo = JSON.parseObject(json, classOf[OrderInfo])
                            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
                        } else {
                            // 不存在
                            cacheOrderDetail(client, orderDetail)
                            Nil
                        }
                }
                
                client.close()
                r
            })
        
    }
    
    // 从mysql反查到user信息
    def joinUser(saleDetail: DStream[SaleDetail]) = {
        // 使用spark-sql读
        val spark = SparkSession.builder()
            .config(ssc.sparkContext.getConf)
            .getOrCreate()
        import spark.implicits._
        
        // 1. 先查询到user_info信息  sparkSql-> df/ds -> rdd
        def readUserInfoes(ids: String) = {
            
            spark
                .read
                .format("jdbc")
                .option("url", "jdbc:mysql://hadoop102:3306/gmall0421?useSSL=false")
                .option("user", "root")
                .option("password", "aaaaaa")
                .option("query", s"select * from user_info where id in (${ids})")
                .load()
                .as[UserInfo]
                .rdd
                .map(userInfo => (userInfo.id, userInfo))
        }
        
        // 2. saleDetail和user信息进行join   saleDetail-> rdd
        saleDetail.transform(rdd => {
            rdd.cache()  // rdd使用多次, 加缓存. 必须加
            // [1, 2, 3]  => '1','2','3'
            val ids = rdd.map(_.user_id).collect().mkString("'", "','", "'") // '1','2','3'
            val userInfoRDD = readUserInfoes(ids) // 每个批次都要第一次user数据
            
            rdd
                .map(saleDetail => (saleDetail.user_id, saleDetail))
                .join(userInfoRDD)
                .map {
                    case (_, (saleDetail, userInfo)) =>
                        saleDetail.mergeUserInfo(userInfo)
                }
            
        })
        
        
    }
    
    override def run(streams: Map[String, DStream[String]]): Unit = {
        val orderInfoStream = streams(Constant.ORDER_INFO_TOPIC)
            .map(json => JSON.parseObject(json, classOf[OrderInfo]))
        val orderDetailStream = streams(Constant.ORDER_DETAIL_TOPIC)
            .map(json => JSON.parseObject(json, classOf[OrderDetail]))
        
        // 1. 对两个流进行join   join leftJoin rightJoin fullJoin
        val saleDetail = joinOrderInfoOrderDetail(orderInfoStream, orderDetailStream)
        // 2. join user
        val saleDetailWithUser = joinUser(saleDetail)
        saleDetailWithUser.print()
    }
}

/*
orderInfo 在redis中的存储方式:
key                              value
"order_info:"+order_id           json字符串


orderDetail 在redis中的存储方式:
key                              value(hash)
"order_detail:"+order_id         field                  value
                                 order_detail_id        json字符串
                                

-----
维度表:
    1. 早于事实表存在
    2. 变化缓慢


 */

/*def joinOrderInfoOrderDetail(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
            val orderInfoStreamWithWindow = orderInfoStream
                .window(Seconds(bachTime * 3), Seconds(bachTime))
                .map(info => (info.id, info))
            val orderDetailStreamWithWindow = orderDetailStream
                .window(Seconds(bachTime * 3), Seconds(bachTime))
                .map(detail => (detail.order_id, detail))
            
            val saleDetailStream = orderInfoStreamWithWindow
                .join(orderDetailStreamWithWindow)
                .map {
                    case (orderId, (orderInfo, orderDetail)) =>
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                }
            
            // saleDetailStream 内部会重复join, 需要去重, 使用redis去重
            saleDetailStream.mapPartitions((saleDetailIt: Iterator[SaleDetail]) => {
                val client: Jedis = RedisUtil.getClient
                // 把order_detail_id写入到redis的set中, 如果返回1, 表示是第一次join(保留), 如果返回0表示不是第一次(过滤)
                val r = saleDetailIt.filter(saleDetail => {
                    client.sadd(s"order_detail_ids: ${saleDetail.dt}", saleDetail.order_detail_id) == 1
                })
                client.close()
                r
            })
        }*/