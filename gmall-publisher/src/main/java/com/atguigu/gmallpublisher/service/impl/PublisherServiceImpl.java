package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {

        //1.创建Map用于存放最终结果数据
        HashMap<String, Long> result = new HashMap<>();

        //2.查询Phoenix,获取分时数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //3.遍历List,将数据放入Map中
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //4.返回结果
        return result;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {

        //1.创建Map用于存放最终结果数据
        HashMap<String, Double> result = new HashMap<>();

        //2.查询Phoenix,获取分时数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //3.遍历List,将数据放入Map中
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        //4.返回结果
        return result;
    }

}
