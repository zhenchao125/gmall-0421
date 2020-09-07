package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.realtime.bean.OrderInfo
import com.atguigu.realtime.gmall.common.Constant
import org.apache.spark.streaming.dstream.DStream
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JDouble, JString}
import org.json4s.jackson.JsonMethods


/**
 * Author atguigu
 * Date 2020/9/7 9:25
 */
object OrderApp extends BaseApp {
    override val topics: Set[String] = Set(Constant.ORDER_INFO_TOPIC)
    override val groupId: String = "OrderApp"
    override val appName: String = "OrderApp"
    override val master: String = "local[2]"
    override val bachTime: Int = 3
    
    object StringToDouble extends CustomSerializer[Double](format => (
        {
            case JString(d) => d.toDouble
        },
        {
            case d: Double => JDouble(d)
        }
    ))
    
    override def run(sourceStream: DStream[String]): Unit = {
        /*sourceStream
            .map(json => JSON.parseObject(json, classOf[OrderInfo]))
            .print()*/
        
        sourceStream
            .map(json => {
                implicit val f = org.json4s.DefaultFormats + StringToDouble
                println(json)
                JsonMethods.parse(json).extract[OrderInfo]
            })
            .print()
        
    }
    
}
