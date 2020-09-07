package com.atguigu.gmall.mock

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.mock.util.{LogUploader, RandomNumUtil, RandomOptions}

object JsonMock {
    
    val startupNum = 100000 // 生成的启动日志的记录数
    val eventNum = 200000 // 生成的事件日志的记录数
    
    // 操作系统的分布
    val osOpts = RandomOptions(("ios", 3), ("android", 7))
    
    // 日志开始时间
    var startDate: Date = _
    // 日志结束时间
    var endDate: Date = _
    
    // 地理位置分布
    val areaOpts = RandomOptions(
        ("beijing", 20), ("shanghai", 20), ("guangdong", 20),
        ("hebei", 5), ("heilongjiang", 5), ("shandong", 5),
        ("tianjin", 5), ("guizhou", 5), ("shangxi", 5),
        ("sichuan", 5), ("xinjiang", 5)
    )
    
    // appId
    val appId = "gmall"
    
    // app 的版本分布
    val versionOpts = RandomOptions(
        ("1.2.0", 50), ("1.1.2", 15),
        ("1.1.3", 30), ("1.1.1", 5))
    
    // 用户行为的分布(事件分布)
    val eventOpts = RandomOptions(
        ("addFavor", 10), ("addComment", 30),
        ("addCart", 20), ("clickItem", 0), ("coupon", 100))
    
    // app 分发渠道分布
    val channelOpts = RandomOptions(
        ("xiaomi", 10), ("huawei", 20), ("wandoujia", 30),
        ("360", 20), ("tencent", 20), ("baidu", 10), ("website", 10))
    
    // 生成模拟数据的时候是否结束退出
    val quitOpts = RandomOptions((true, 5), (false, 95))
    
    // 模拟出来一条启动日志
    def initOneStartupLog(): String = {
        /*
        `logType` string   COMMENT '日志类型',
        `mid` string COMMENT '设备唯一标识',
        `uid` string COMMENT '用户标识',
        `os` string COMMENT '操作系统', ,
        `appId` string COMMENT '应用id', ,
        `version` string COMMENT '版本号',
        `ts` bigint COMMENT '启动时间',    考虑每个终端的时间的不准群性, 时间是将来在服务器端来生成
        `area` string COMMENT '城市'
        `channel` string COMMENT '渠道'
         */
        val mid: String = "mid_" + RandomNumUtil.randomInt(1, 10)
        val uid: String = "" + RandomNumUtil.randomInt(1, 10000)
        val os: String = osOpts.getRandomOption()
        val appId: String = this.appId
        val area: String = areaOpts.getRandomOption()
        val version: String = versionOpts.getRandomOption()
        val channel: String = channelOpts.getRandomOption()
        
        val obj = new JSONObject()
        obj.put("logType", "startup")
        obj.put("mid", mid)
        obj.put("uid", uid)
        obj.put("os", os)
        obj.put("appId", appId)
        obj.put("area", area)
        obj.put("channel", channel)
        obj.put("version", version)
        // 返回 json 格式字符串
        obj.toJSONString
    }
    
    // 模拟出来一条事件日志  参数: json 格式的启动日志
    def initOneEventLog(startupLogJson: String) = {
        /*`
        logType` string   COMMENT '日志类型',
        `mid` string COMMENT '设备唯一标识',
        `uid` string COMMENT '用户标识',
        `os` string COMMENT '操作系统',
        `appId` string COMMENT '应用id',
        `area` string COMMENT '地区' ,
        `eventId` string COMMENT '事件id',
        `pageId` string COMMENT '当前页',
        `nextPageId` string COMMENT '跳转页',
        `itemId` string COMMENT '商品编号',
        `ts` bigint COMMENT '时间'
         */
        val startupLogObj: JSONObject = JSON.parseObject(startupLogJson)
        
        val eventLogObj = new JSONObject()
        eventLogObj.put("logType", "event")
        eventLogObj.put("mid", startupLogObj.getString("mid"))
        eventLogObj.put("uid", startupLogObj.getString("uid"))
        eventLogObj.put("os", startupLogObj.getString("os"))
        eventLogObj.put("appId", this.appId)
        eventLogObj.put("area", startupLogObj.getString("area"))
        eventLogObj.put("eventId", eventOpts.getRandomOption())
        eventLogObj.put("pageId", RandomNumUtil.randomInt(1, 50))
        eventLogObj.put("nextPageId", RandomNumUtil.randomInt(1, 50))
        eventLogObj.put("itemId", RandomNumUtil.randomInt(1, 50))
       
        eventLogObj.toJSONString
    }
    
    // 开始生成日志
    def generateLog(): Unit = {
        (0 to startupNum).foreach(_ => {
            // 生成一条启动日志
            val oneStartupLog: String = initOneStartupLog()
            // 发送启动日志
            LogUploader.sendLog(oneStartupLog)
//            println(oneStartupLog)
            // 模拟出来多条事件日志
            while (!quitOpts.getRandomOption()) {
                // 生成一条事件日志
                val oneEventLog: String = initOneEventLog(oneStartupLog)
                // 发送事件日志
                LogUploader.sendLog(oneEventLog)
//                println(oneEventLog)
                Thread.sleep(100)
            }
            Thread.sleep(1000)
        })
    }
    
    def main(args: Array[String]): Unit = {
       
        generateLog()
    }
}
