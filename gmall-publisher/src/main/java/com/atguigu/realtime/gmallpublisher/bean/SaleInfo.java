package com.atguigu.realtime.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/9/12 14:15
 */
public class SaleInfo {
    private Long total;
    private List<Stat> stats = new ArrayList<>();
    private List<Map> details;

    public SaleInfo() {
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public List<Stat> getStats() {
        return stats;
    }

    public void addStat(Stat stat) {
        this.stats.add(stat);
    }

    public List<Map> getDetails() {
        return details;
    }

    public void setDetails(List<Map> details) {
        this.details = details;
    }
}
