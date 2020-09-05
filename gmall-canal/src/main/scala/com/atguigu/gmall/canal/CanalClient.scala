package com.atguigu.gmall.canal

import java.net.{InetSocketAddress, SocketAddress}
import java.util

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}

/**
 * Author atguigu
 * Date 2020/9/5 14:34
 * 从canal服务器的某个实例中读取数据
 */
object CanalClient {
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
            if(entries != null && !entries.isEmpty){
                println(entries)
            }else{
                System.out.println("没有拉到数据, 3s后继续拉取...");
                Thread.sleep(3000)
            }
        }
        
        // 3. 解析数据
    }
}
