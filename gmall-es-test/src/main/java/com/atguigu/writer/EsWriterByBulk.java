package com.atguigu.writer;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriterByBulk {

    public static void main(String[] args) throws IOException {

        //1.创建连接的工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.指定连接的地址
        HttpClientConfig httpClientConfig = new HttpClientConfig
                .Builder("http://hadoop102:9200")
                .build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //4.创建Bulk.Builder对象
        Bulk.Builder builder = new Bulk.Builder();

        //5.创建多个Index对象并添加至builder
        Movie movie1 = new Movie("003", "零零七");
        Movie movie2 = new Movie("004", "阿甘正传");
        Movie movie3 = new Movie("005", "教父");

        Bulk.Builder builder1 = builder
                .addAction(new Index.Builder(movie1).id("1003").build())
                .addAction(new Index.Builder(movie2).id("1004").build())
                .addAction(new Index.Builder(movie3).id("1005").build());

        //6.创建Bulk对象
        Bulk bulk = builder1
                .defaultIndex("movie2")
                .defaultType("_doc").build();

        //7.执行插入操作
        jestClient.execute(bulk);

        //8.关闭连接
        jestClient.shutdownClient();

    }

}
