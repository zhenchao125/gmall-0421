package com.atguigu.realtime.gmallpublisher.bean;

/**
 * @Author lzc
 * @Date 2020/9/12 14:09
 */
public class Option {
    private String name;
    private Long value;

    public Option() {
    }

    public Option(String name, Long value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
