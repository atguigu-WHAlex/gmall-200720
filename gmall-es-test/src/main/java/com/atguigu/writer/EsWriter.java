package com.atguigu.writer;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriter {

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

        //4.创建Index对象
        Movie movie = new Movie("002", "暴徒");
        Index index = new Index.Builder(movie)
                .index("movie2")
                .type("_doc")
                .id("aaaa")
                .build();

        //5.执行插入操作
        jestClient.execute(index);

        //6.关闭连接
        jestClient.shutdownClient();

    }

}
