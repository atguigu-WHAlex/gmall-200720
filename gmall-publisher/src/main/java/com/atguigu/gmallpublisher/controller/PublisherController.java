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

        //5.创建交易额Map
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", publisherService.getOrderAmount(date));

        //6.将日活及新增设备Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        //7.将集合转换为JSON字符串返回
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date) {

        //1.创建Map用于存放结果数据
        HashMap<String, Map> result = new HashMap<>();

        //获取昨天日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        //声明今天以及昨天数据的Map
        Map todayMap = null;
        Map yesterdayMap = null;

        //判断当前查询的是哪个分时数据(日活,新增设备,GMV)
        if ("dau".equals(id)) {
            //日活
            //2.查询今天的分时数据
            todayMap = publisherService.getDauTotalHourMap(date);
            //3.查询昨天的分时数据
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        } else if ("order_amount".equals(id)) {
            //GMV
            //2.查询今天的分时数据
            todayMap = publisherService.getOrderAmountHour(date);
            //3.查询昨天的分时数据
            yesterdayMap = publisherService.getOrderAmountHour(yesterday);
        }

        //4.将两天的分时数据放入集合
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        //5.将Map转换为JSON字符串返回
        return JSONObject.toJSONString(result);
    }

}
