package com.atguigu.realtime.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2020/9/12 14:11
 */
public class Stat {
    private String title;
    private List<Option> options = new ArrayList<>();

    public Stat() {
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void addOption(Option opt){
        options.add(opt);
    }

}
