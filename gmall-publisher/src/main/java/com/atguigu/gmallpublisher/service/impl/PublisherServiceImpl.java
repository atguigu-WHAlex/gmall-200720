package com.atguigu.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

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

    @Override
    public String getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {

        //1.构建查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //1.1 添加全值匹配过滤条件
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("dt", date);
        boolQueryBuilder.filter(termQueryBuilder);

        //1.2 添加分词匹配过滤条件
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        //1.3 添加年龄聚合组
        String ageGroupName = "countByAge";
        TermsBuilder ageAgg = AggregationBuilders.terms(ageGroupName).field("user_age").size(1000);
        searchSourceBuilder.aggregation(ageAgg);

        //1.4 添加性别聚合组
        String genderGroupName = "countByGender";
        TermsBuilder genderAgg = AggregationBuilders.terms(genderGroupName).field("user_gender").size(3);
        searchSourceBuilder.aggregation(genderAgg);

        //1.5 分页
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        //2.执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("gmall2020_sale_detail-query")
                .addType("_doc")
                .build();
        SearchResult searchResult = jestClient.execute(search);

        //3.解析查询结果

        //3.1 创建Map用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();

        //3.2 处理总数
        Long total = searchResult.getTotal();

        //3.3 处理明细数据
        ArrayList<Map> details = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }


        MetricAggregation aggregations = searchResult.getAggregations();
        //3.4 处理性别聚合组
        TermsAggregation genderTermsAggregation = aggregations.getTermsAggregation(genderGroupName);
        //定义男有多少人
        Long maleCount = 0L;
        for (TermsAggregation.Entry entry : genderTermsAggregation.getBuckets()) {
            if ("M".equals(entry.getKey())) {
                maleCount = entry.getCount();
            }
        }
        //求出性别比例
        double maleRatio = (Math.round(maleCount * 1000D / total)) / 10D;
        double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;

        //创建并别比例的Option对象
        Option maleOpt = new Option("男", maleRatio);
        Option femaleOpt = new Option("女", femaleRatio);

        //创建集合存放性别占比的Option对象
        ArrayList<Option> genderOpts = new ArrayList<>();
        genderOpts.add(maleOpt);
        genderOpts.add(femaleOpt);

        //创建性别占比的Stat对象
        Stat genderStat = new Stat("用户性别占比", genderOpts);

        //3.5 处理年龄聚合组
        TermsAggregation ageTermsAggregation = aggregations.getTermsAggregation(ageGroupName);

        //定义20岁以下以及30岁以上的人数
        Long lower20 = 0L;
        Long upper30 = 0L;
        for (TermsAggregation.Entry entry : ageTermsAggregation.getBuckets()) {
            if (Integer.parseInt(entry.getKey()) < 20) {
                lower20 += entry.getCount();
            } else if (Integer.parseInt(entry.getKey()) >= 30) {
                upper30 += entry.getCount();
            }
        }

        //计算各个年龄段的占比
        double lower20Ratio = (Math.round(lower20 * 1000D / total)) / 10D;
        double upper30Ratio = (Math.round(upper30 * 1000D / total)) / 10D;
        double between20And30Ratio = Math.round((100D - lower20Ratio - upper30Ratio) * 10D) / 10D;

        //创建年龄段的Option对象
        ArrayList<Option> ageOpts = new ArrayList<>();
        Option lower20Opt = new Option("20岁以下", lower20Ratio);
        Option between20And30Opt = new Option("20岁到30岁", between20And30Ratio);
        Option upper30Opt = new Option("30岁及30岁以上", upper30Ratio);
        ageOpts.add(lower20Opt);
        ageOpts.add(between20And30Opt);
        ageOpts.add(upper30Opt);

        //创建年龄占比的Stat对象
        Stat ageStat = new Stat("用户年龄占比", ageOpts);

        //将两个占比数据放入集合
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //4.返回数据
        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", details);

        return JSON.toJSONString(result);
    }

}
