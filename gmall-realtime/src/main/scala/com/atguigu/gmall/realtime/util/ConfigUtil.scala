package com.atguigu.gmall.realtime.util

import java.util.Properties

/**
 * Author atguigu
 * Date 2020/9/4 9:44
 */
object ConfigUtil {
    
    
    val is = ClassLoader.getSystemResourceAsStream("config.properties")
    val properties = new Properties()
    properties.load(is)
    
    def getConf(name: String) = {
        properties.getProperty(name)
    }
}
