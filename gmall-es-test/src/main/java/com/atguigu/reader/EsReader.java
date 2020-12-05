package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReader {

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

        //4.创建Search对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //4.1 添加查询条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //4.1.1 添加全值匹配过滤条件
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("class_id", "0720");
        boolQueryBuilder.filter(termQueryBuilder);

        //4.1.2 添加分词匹配过滤条件
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo2", "球");
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        //4.2 添加聚合组
        MaxAggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
        searchSourceBuilder.aggregation(aggregationBuilder);

        //4.3 分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);

//        Search search = new Search.Builder("{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"class_id\": \"0720\"\n" +
//                "        }\n" +
//                "      },\n" +
//                "      \"must\": [\n" +
//                "        {\n" +
//                "          \"match\": {\n" +
//                "            \"favo2\": \"球\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      ]\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"aggs\": {\n" +
//                "    \"maxAge\": {\n" +
//                "      \"max\": {\n" +
//                "        \"field\": \"age\"\n" +
//                "      }\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"from\": 0,\n" +
//                "  \"size\": 10\n" +
//                "}")
//                .addIndex("stu1")
//                .addType("_doc")
//                .build();

        //5.执行查询操作
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        System.out.println(searchSourceBuilder);
        SearchResult searchResult = jestClient.execute(search);

        //6.解析searchResult

        //6.1 获取总数
        System.out.println("查询结果总数:" + searchResult.getTotal());

        //6.2 获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println(hit.source);
        }

        //6.3 获取聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();
        MaxAggregation maxAge = aggregations.getMaxAggregation("maxAge");
        System.out.println("最大年纪为:" + maxAge.getMax());

        //7.关闭连接
        jestClient.shutdownClient();
    }

}
