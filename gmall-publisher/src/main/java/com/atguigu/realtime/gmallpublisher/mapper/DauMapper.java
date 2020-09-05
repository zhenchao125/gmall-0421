package com.atguigu.realtime.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    /**
     * 返回日活总数
     *
     * @param date
     * @return
     */
    Long getDau(String date);

    List<Map<String, Object>> getHourDau(String date);

}
    /*

+----------+-----------+
| LOGHOUR  | COUNT(1)  |
+----------+-----------+
| 10       | 3         |
| 14       | 7         |
| 16       | 39        |
+----------+-----------+

     */

