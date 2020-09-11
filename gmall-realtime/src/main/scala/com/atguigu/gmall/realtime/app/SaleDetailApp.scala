package com.atguigu.gmall.realtime.app
import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.realtime.gmall.common.Constant
import org.apache.spark.streaming.dstream.DStream

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
    
    override def run(streams: Map[String, DStream[String]]): Unit = {
        val orderInfoStream = streams(Constant.ORDER_INFO_TOPIC)
            .map(json => JSON.parseObject(json, classOf[OrderInfo]))
        val orderDetailStream = streams(Constant.ORDER_DETAIL_TOPIC)
            .map(json => JSON.parseObject(json, classOf[OrderDetail]))
        
        // 1. 对两个流进行join   join leftJoin rightJoin fullJoin
        
    }
}
