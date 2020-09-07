package com.atguigu.realtime.gmallpublisher.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderMapper {
    BigDecimal getTotalAmount(String date);


    List<Map<String, Object>> getHourTotalAmount(String date);
}
