package com.atguigu.realtime.gmallpublisher.service;

import java.math.BigDecimal;
import java.util.Map;

public interface PublisherService {
    Long getDau(String date);

    Map<String, Long> getHourDau(String date);


    BigDecimal getTotalAmount(String date);

    Map<String, BigDecimal> getHourTotalAmount(String date);
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