package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {

    //获取日活总数
    public Integer getDauTotal(String date);

    //获取日活分时数据
    public Map getDauTotalHourMap(String date);

    //获取交易额总数
    public Double getOrderAmount(String date);

    //获取交易额分时数据
    public Map getOrderAmountHour(String date);

    //获取销售明细数据
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException;

}
