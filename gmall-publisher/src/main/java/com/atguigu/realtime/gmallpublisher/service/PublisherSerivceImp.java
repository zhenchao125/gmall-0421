package com.atguigu.realtime.gmallpublisher.service;

import com.atguigu.realtime.gmallpublisher.mapper.DauMapper;
import com.atguigu.realtime.gmallpublisher.mapper.OrderMapper;
import com.atguigu.realtime.gmallpublisher.util.ESUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/9/4 15:49
 */
@Service
class PublisherServiceImp implements PublisherService {

    @Autowired
    DauMapper dau;

    @Override
    public Long getDau(String date) {
        return dau.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDau = dau.getHourDau(date);

        HashMap<String, Long> result = new HashMap<>();

        for (Map<String, Object> map : hourDau) {
            String hour = map.get("LOGHOUR").toString();
            Long count = (Long) map.get("COUNT");
            result.put(hour, count);
        }

        return result;
    }

    @Autowired
    OrderMapper order;

    @Override
    public BigDecimal getTotalAmount(String date) {
        return order.getTotalAmount(date);
    }

    @Override
    public Map<String, BigDecimal> getHourTotalAmount(String date) {
        List<Map<String, Object>> hourAmount = order.getHourTotalAmount(date);

        HashMap<String, BigDecimal> result = new HashMap<>();

        for (Map<String, Object> map : hourAmount) {
            String hour = map.get("CREATE_HOUR").toString();
            BigDecimal sum = (BigDecimal) map.get("SUM");
            result.put(hour, sum);
        }

        return result;
    }

    @Override
    public Map<String, Object> getSaleDetailAndAgg(String date,
                                                   String keyword,
                                                   int startpage,
                                                   int size) throws IOException {
        // 1. 去es查询数据
        // 1.1 得到es客户端
        JestClient client = ESUtil.getClient();

        Search.Builder builder = new Search.Builder(ESUtil.getDSL(date, keyword, startpage, size))
                .addIndex("gmall0421_sale_detail")
                .addType("_doc");
        SearchResult searchResult = client.execute(builder.build());
        client.close();
        // 2. 解析查询到的数据
        HashMap<String, Object> result = new HashMap<>();
        // 2.1 获取总数
        Long total = searchResult.getTotal();
        result.put("total", total);
        // 2.2 获取详情
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        ArrayList<HashMap> details = new ArrayList<>();
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            details.add(source);
        }
        result.put("details", details);
        // 3. 把数据封装到Map中,返回给Controller
        // 3.1 总数  详情   年龄聚合   性别聚合
        // 3.1 性别聚合结果  Map["F"-> 38, "M"->21]
        List<TermsAggregation.Entry> genderBuckets = searchResult
                .getAggregations()
                .getTermsAggregation("group_by_user_gender")
                .getBuckets();
        HashMap<String, Long> genderAgg = new HashMap<>();
        for (TermsAggregation.Entry bucket : genderBuckets) {
            String gender = bucket.getKey();
            Long count = bucket.getCount();
            genderAgg.put(gender, count);
        }
        result.put("genderAgg", genderAgg);
        // 3.2 年龄聚合结果
        List<TermsAggregation.Entry> ageBuckets = searchResult
                .getAggregations()
                .getTermsAggregation("group_by_user_age")
                .getBuckets();
        HashMap<String, Long> ageAgg = new HashMap<>();
        for (TermsAggregation.Entry bucket : ageBuckets) {
            String age = bucket.getKey();
            Long count = bucket.getCount();
            ageAgg.put(age, count);
        }
        result.put("ageAgg", ageAgg);
        return result;
    }

}
/*

+----------+-----------+
| LOGHOUR  | COUNT(1)  |
+----------+-----------+
| 10       | 3         |
| 14       | 7         |
| 16       | 39        |
+----------+-----------+
List[Map("loghour"->10, "count"-> 3), Map("loghour"->14, "count"-> 7),...]

转成
Map["10"-> 3, "14"->7, ...]

 */