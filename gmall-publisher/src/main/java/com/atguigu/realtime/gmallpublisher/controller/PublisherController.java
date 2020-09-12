package com.atguigu.realtime.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.realtime.gmallpublisher.bean.Option;
import com.atguigu.realtime.gmallpublisher.bean.SaleInfo;
import com.atguigu.realtime.gmallpublisher.bean.Stat;
import com.atguigu.realtime.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/9/4 15:53
 */
@RestController
public class PublisherController {
    //http://localhost:8070/realtime-total?date=2020-02-11

    @Autowired
    PublisherService service;

    @GetMapping("/realtime-total")
    public String realtimeTotal(String date) {
        Long dau = service.getDau(date);

        List<Map<String, Object>> result = new ArrayList<>();

        Map<String, Object> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", dau);
        result.add(map1);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", 233);
        result.add(map2);

        Map<String, Object> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", service.getTotalAmount(date));
        result.add(map3);

        return JSON.toJSONString(result);
    }


    // http://localhost:8070/realtime-hour?id=dau&date=2020-02-11
    // http://localhost:8070/realtime-hour?id=order_amount&date=2020-02-14
    @GetMapping("/realtime-hour")
    public String realtimeHour(String id, String date) {
        if ("dau".equals(id)) {
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));

            HashMap<String, Map<String, Long>> result = new HashMap<>();
            result.put("yesterday", yesterday);
            result.put("today", today);

            return JSON.toJSONString(result);
        } else if ("order_amount".equals(id)) {
            Map<String, BigDecimal> today = service.getHourTotalAmount(date);
            Map<String, BigDecimal> yesterday = service.getHourTotalAmount(getYesterday(date));

            HashMap<String, Map<String, BigDecimal>> result = new HashMap<>();
            result.put("yesterday", yesterday);
            result.put("today", today);
            return JSON.toJSONString(result);
        }
        return "ok";

    }


    //  http://localhost:8070/sale_detail?date=2019-05-20&&startpage=1&&size=5&&keyword=手机小米
    @GetMapping("/sale_detail")
    public String saleDetail(String date, int startpage, int size, String keyword) throws IOException {
        Map<String, Object> saleDetailAndAgg = service.getSaleDetailAndAgg(date, keyword, startpage, size);
        // 最终结果
        SaleInfo saleInfo = new SaleInfo();
        // 1. 设置total
        Long total = (Long) saleDetailAndAgg.get("total");
        saleInfo.setTotal(total);
        // 2. 设置详情
        List<Map> details = (List<Map>) saleDetailAndAgg.get("details");
        saleInfo.setDetails(details);
        // 3. 设置饼图
        // 3.1 性别饼图
        Map<String, Long> genderAgg = (Map<String, Long>) saleDetailAndAgg.get("genderAgg");
        Stat genderStat = new Stat();
        saleInfo.addStat(genderStat);
        // 3.1.1 title
        genderStat.setTitle("用户性别占比");
        // 3.1.2 添加饼图的组成部分
        for (Map.Entry<String, Long> entry : genderAgg.entrySet()) {
            Option opt = new Option();
            opt.setName(entry.getKey().equals("F") ? "女" : "男");
            opt.setValue(entry.getValue());
            genderStat.addOption(opt);
        }
        // 3.2 设置年龄的饼图
        Map<String, Long> ageAgg = (Map<String, Long>) saleDetailAndAgg.get("ageAgg");
        Stat ageStat = new Stat();
        saleInfo.addStat(ageStat);
        // 3.2.1 title
        genderStat.setTitle("用户年龄占比");
        // 3.2.2 组成部分
        ageStat.addOption(new Option("20岁以下", 0L));
        ageStat.addOption(new Option("20岁到30岁", 0L));
        ageStat.addOption(new Option("30岁及以上", 0L));
        for (String key : ageAgg.keySet()) {
            int age = Integer.parseInt(key);
            Long value = ageAgg.get(key);
            if (age < 20) {
                Option stat = ageStat.getOptions().get(0);
                stat.setValue(stat.getValue() + value);
            } else if (age < 30) {
                Option stat = ageStat.getOptions().get(1);
                stat.setValue(stat.getValue() + value);
            } else {
                Option stat = ageStat.getOptions().get(2);
                stat.setValue(stat.getValue() + value);
            }
        }

        return JSON.toJSONString(saleInfo).replace("details", "detail") ;
    }

    private String getYesterday(String date) {
        return LocalDate.parse(date).minusDays(1).toString();
    }

}
/*
[{"id":"dau","name":"新增日活","value":1200},
{"id":"new_mid","name":"新增设备","value":233 },
{"id":"order_amount","name":"新增交易额","value":1000.2 }]



{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
"today":{"12":38,"13":1233,"17":123,"19":688 }}


 */