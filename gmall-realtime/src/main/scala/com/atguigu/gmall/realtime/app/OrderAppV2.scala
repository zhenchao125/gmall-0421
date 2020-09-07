package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.OrderInfo
import com.atguigu.realtime.gmall.common.Constant
import org.apache.spark.streaming.dstream.DStream


/**
 * Author atguigu
 * Date 2020/9/7 9:25
 */
object OrderAppV2 extends BaseApp {
    override val topics: Set[String] = Set(Constant.ORDER_INFO_TOPIC)
    override val groupId: String = "OrderApp"
    override val appName: String = "OrderApp"
    override val master: String = "local[2]"
    override val bachTime: Int = 3
    
    
    override def run(sourceStream: DStream[String]): Unit = {
        sourceStream
            .map(json => JSON.parseObject(json, classOf[OrderInfo]))
            .foreachRDD(rdd => {
                import org.apache.phoenix.spark._
                rdd.saveToPhoenix("gmall_order_info0421",
                    Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                    zkUrl = Option("hadoop102,hadoop103,hadoop104:2181"))
            })
    }
    
}
