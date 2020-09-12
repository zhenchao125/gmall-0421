package com.atguigu.realtime.gmallpublisher.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

public interface PublisherService {
    Long getDau(String date);

    Map<String, Long> getHourDau(String date);


    BigDecimal getTotalAmount(String date);

    Map<String, BigDecimal> getHourTotalAmount(String date);


    /**
     * 从es查询指定的数据
     *
     * @param date
     * @param keyword
     * @param startpage
     * @param size
     * @return
     */
    Map<String, Object> getSaleDetailAndAgg(String date,
                                            String keyword,
                                            int startpage,
                                            int size) throws IOException;

}
/*
GET /gmall0421_sale_detail/_search
{
  "query": {
    "bool": {
      "filter": {
        "term": {
          "dt": "2020-09-12"
        }
      },
      "must": [
        {"match": {
          "sku_name": {
            "query": "手机小米",
            "operator": "and"
          }
        }}
      ]
    }
  },
  "aggs": {
    "group_by_user_gender": {
      "terms": {
        "field": "user_gender",
        "size": 2
      }
    },
    "group_by_user_age": {
      "terms": {
        "field": "user_age",
        "size": 200
      }
    }
  },
  "from": 0
  , "size": 5
}





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