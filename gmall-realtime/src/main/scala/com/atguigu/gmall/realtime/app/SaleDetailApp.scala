package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall.realtime.util.RedisUtil
import com.atguigu.realtime.gmall.common.Constant
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/9/11 9:05
 */
object SaleDetailApp extends BaseAppV2 {
    override val topics: Set[String] = Set(Constant.ORDER_INFO_TOPIC, Constant.ORDER_DETAIL_TOPIC)
    override val groupId: String = "SaleDetailApp"
    override val master: String = "local[2]"
    override val appName: String = "SaleDetailApp"
    override val bachTime: Int = 3
    
    def joinOrderInfoOrderDetail(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
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
    }
    
    override def run(streams: Map[String, DStream[String]]): Unit = {
        val orderInfoStream = streams(Constant.ORDER_INFO_TOPIC)
            .map(json => JSON.parseObject(json, classOf[OrderInfo]))
        val orderDetailStream = streams(Constant.ORDER_DETAIL_TOPIC)
            .map(json => JSON.parseObject(json, classOf[OrderDetail]))
        
        // 1. 对两个流进行join   join leftJoin rightJoin fullJoin
        val saleDetail = joinOrderInfoOrderDetail(orderInfoStream, orderDetailStream)
        saleDetail.print()
    }
}
