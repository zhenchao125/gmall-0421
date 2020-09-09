package com.atguigu.gmall.realtime.app

import java.{util => ju}

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.{AlertInfo, EventLog}
import com.atguigu.realtime.gmall.common.Constant
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds}

import scala.util.control.Breaks._

/**
 * Author atguigu
 * Date 2020/9/7 14:16
 */
object AlertApp extends BaseApp {
    override val topics: Set[String] = Set(Constant.EVENT_TOPIC)
    override val groupId: String = "AlertApp"
    override val master: String = "local[2]"
    override val appName: String = "AlertApp"
    override val bachTime: Int = 3
    
    override def run(sourceStream: DStream[String]): Unit = {
        val eventLogStream = sourceStream
            .map(json => {
                val log = JSON.parseObject(json, classOf[EventLog])
                (log.mid, log)
            })
            .window(Minutes(5), Seconds(6))
        
        val alertInfoStream = eventLogStream
            .groupByKey()
            .map {
                case (mid, it: Iterable[EventLog]) =>
                    // 对 it变量, 计算想要数据
                    // 1. 存储在当前设备领取优惠券的用户id
                    val uids = new ju.HashSet[String]()
                    // 2. 在当前设备的所有操作事件
                    val events = new ju.ArrayList[String]()
                    // 3. 存储优惠券所在的商品
                    val items = new ju.HashSet[String]()
                    
                    //4. 一个boolean值, 表示是否浏览商品
                    var isBrowser = false
                    breakable {
                        it.foreach(eventLog => {
                            events.add(eventLog.eventId)
                            eventLog.eventId match {
                                // 如果是优惠券
                                case "coupon" =>
                                    uids.add(eventLog.uid)
                                    items.add(eventLog.itemId)
                                // 如果浏览商品
                                case "clickItem" =>
                                    // 5分种内有浏览商品
                                    isBrowser = true
                                    break
                                case _ =>
                            }
                            
                        })
                    }
                    // (true, 具体语境信息)
                    (uids.size() >= 3 && !isBrowser, AlertInfo(mid, uids, items, events, System.currentTimeMillis()))
            }
        
        alertInfoStream
            .filter(_._1)
            .map(_._2)
            .foreachRDD(rdd => {
                println("打印时间戳开始")
                // 把数据写入到es
                rdd.collect().foreach(println)
                
                println("打印时间戳结束")
            })
    }
}

/*
同一设备，
5分钟内三次及以上用不同账号登录并领取优惠劵，
并且在登录到领劵过程中没有浏览商品。
同时达到以上要求则产生一条预警日志。 同一设备，每分钟只记录一次预警。

分析:
    同一设备:  按照mid分组
    5分钟内, 每个6秒更新一次预警: 窗口: 窗口长度  窗口步长
    
    三次及以上用不同账号登录并领取优惠劵:
        统计领取优惠券的用户数
    没有浏览商品
    
    
    同一设备，每分钟只记录一次预警。
        让es来负责去重
    
    
    
 */