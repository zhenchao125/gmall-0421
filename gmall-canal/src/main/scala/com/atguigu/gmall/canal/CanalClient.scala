package com.atguigu.gmall.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.atguigu.realtime.gmall.common.Constant
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Author atguigu
 * Date 2020/9/5 14:34
 * 从canal服务器的某个实例中读取数据
 */
object CanalClient {
    
    def handleRowData(rowDataList: util.List[CanalEntry.RowData],
                      tableName: String,
                      eventType: CanalEntry.EventType) = {
        if (rowDataList.size() > 0 && tableName == "order_info" && eventType == CanalEntry.EventType.INSERT) {
            handleData(rowDataList, Constant.ORDER_INFO_TOPIC)
        } else if (rowDataList.size() > 0 && tableName == "order_detail" && eventType == CanalEntry.EventType.INSERT) {
            handleData(rowDataList, Constant.ORDER_DETAIL_TOPIC)
        }
    }
    
    private def handleData(rowDataList: util.List[CanalEntry.RowData], topic: String): Unit = {
        for (rowData <- rowDataList.asScala) {
            val obj = new JSONObject()
            
            val columns: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
            for (column <- columns.asScala) {
                obj.put(column.getName, column.getValue)
            }
            // 写到kafka
            // 1. 创建一个生产者
            // 2. 写
            new Thread() {
                override def run(): Unit = {
                    Thread.sleep(new Random().nextInt(20 * 1000))
                    MyKafkaUtil.send(topic, obj.toJSONString)
                }
            }.start()
        }
    }
    
    def main(args: Array[String]): Unit = {
        // 1. 连接到canal
        val addr = new InetSocketAddress("hadoop102", 11111)
        val conn: CanalConnector = CanalConnectors.newSingleConnector(addr, "example", "", "")
        // 1.1 连接
        conn.connect()
        // 1.2 订阅数据库和表
        conn.subscribe("gmall0421.*")
        // 2. 获取数据
        // 2.1 拉取数据. 最多拉取100条sql导致变化的数据
        while (true) {
            val msg: Message = conn.get(100)
            val entries: util.List[CanalEntry.Entry] = msg.getEntries
            if (entries != null && !entries.isEmpty) {
                for (entry <- entries.asScala) {
                    if (entry != null && entry.hasEntryType && entry.getEntryType == CanalEntry.EntryType.ROWDATA) {
                        val storeValue: ByteString = entry.getStoreValue
                        val rowChange: RowChange = RowChange.parseFrom(storeValue)
                        val rowDataList: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
                        // 只处理部分表
                        handleRowData(rowDataList, entry.getHeader.getTableName, rowChange.getEventType)
                    }
                }
            } else {
                System.out.println("没有拉到数据, 3s后继续拉取...");
                Thread.sleep(3000)
            }
        }
        
        // 3. 解析数据
    }
}
