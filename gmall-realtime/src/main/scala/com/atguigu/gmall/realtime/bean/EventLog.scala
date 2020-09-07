package com.atguigu.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class EventLog(mid: String,
                    uid: String,
                    appId: String,
                    area: String,
                    os: String,
                    logType: String,
                    eventId: String,
                    pageId: String,
                    nextPageId: String,
                    itemId: String,
                    ts: Long,
                    var logDate: String = null,
                    var logHour: String = null) {
    private val date = new Date(ts)
    logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
    logHour = new SimpleDateFormat("HH").format(date)
}
