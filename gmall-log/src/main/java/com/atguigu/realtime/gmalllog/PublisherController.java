package com.atguigu.realtime.gmalllog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @Author lzc
 * @Date 2020/9/2 11:30
 */
/*@Controller
@ResponseBody*/
@RestController  // == @Controller + @ResponseBody
public class PublisherController {
    @PostMapping("/log")
    public String doLog(String log) {
        // 1. 添加时间戳
        log = addTs(log);
        System.out.println(log);
        // 2. 落盘(给离线需求使用)

        // 3. 把数据发动到kafka


        return "ok";
    }

    /**
     * 给日志添时间戳
     *
     * @param log
     */
    private String addTs(String log) {
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts", System.currentTimeMillis());
        return obj.toJSONString();
    }
}
