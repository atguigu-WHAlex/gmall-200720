package com.atguigu.gmalllogger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
//@RestController = @Controller+@ResponseBody
@RestController
public class LoggerController {

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
        System.out.println(logString);

        //1.将数据落盘

        //2.将数据写入Kafka

        return "success";
    }


}
