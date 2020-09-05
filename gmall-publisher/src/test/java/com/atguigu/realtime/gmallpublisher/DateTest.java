package com.atguigu.realtime.gmallpublisher;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * @Author lzc
 * @Date 2020/9/5 9:50
 */
public class DateTest {
    public static void main(String[] args) {
        LocalDate now = LocalDate.now();
//        System.out.println(now.toString());
        System.out.println(now.getYear());

        LocalTime time = LocalTime.now();

        System.out.println(LocalDateTime.now().toString());
    }
}
