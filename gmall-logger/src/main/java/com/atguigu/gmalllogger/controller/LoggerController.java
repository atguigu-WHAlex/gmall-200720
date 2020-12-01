package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@Controller
//@RestController = @Controller+@ResponseBody
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("test1")
    // @ResponseBody  //表示返回值不是页面
    public String test1() {
        System.out.println("aaaa");
        return "success";
    }

    @RequestMapping("test2")
    // @ResponseBody  //表示返回值不是页面
    public String test2(@RequestParam("name") String nn,
                        @RequestParam("age") String age) {
        System.out.println(nn + ":" + age);
        return "success";
    }

    @RequestMapping("log")
    // @ResponseBody  //表示返回值不是页面
    public String getLogger(@RequestParam("logString") String logString) {
//        System.out.println(logString);

        //0.添加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        //1.将数据落盘
        log.info(jsonObject.toString());

        //2.将数据写入Kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            //启动日志,将数据写入启动日志主题
            kafkaTemplate.send(GmallConstant.GMALL_STARTUP_TOPIC, jsonObject.toString());

        } else {
            //事件日志,将数据写入事件日志主题
            kafkaTemplate.send(GmallConstant.GMALL_EVENT_TOPIC, jsonObject.toString());
        }

        return "success";
    }


}
