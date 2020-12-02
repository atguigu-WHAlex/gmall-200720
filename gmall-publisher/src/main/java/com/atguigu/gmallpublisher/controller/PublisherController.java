package com.atguigu.gmallpublisher.controller;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {

        //1.定义集合,用于存放最终结果数据
        ArrayList<Map> result = new ArrayList<>();

        //2.查询日活总数
        Integer dauTotal = publisherService.getDauTotal(date);

        //3.创建日活Map
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //4.创建新增用户的Map
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        //5.将日活及新增设备Map放入集合
        result.add(dauMap);
        result.add(newMidMap);

        //6.将集合转换为JSON字符串返回
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date) {

        //1.创建Map用于存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        //2.查询今天的分时数据
        Map todayMap = publisherService.getDauTotalHourMap(date);

        //3.查询昨天的分时数据
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        Map yesterdayMap = publisherService.getDauTotalHourMap(yesterday);

        //4.将两天的分时数据放入集合
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //5.将Map转换为JSON字符串返回
        return JSONObject.toJSONString(result);
    }

}
