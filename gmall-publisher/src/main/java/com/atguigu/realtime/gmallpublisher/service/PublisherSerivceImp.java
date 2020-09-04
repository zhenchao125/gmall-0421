package com.atguigu.realtime.gmallpublisher.service;

import com.atguigu.realtime.gmallpublisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Author lzc
 * @Date 2020/9/4 15:49
 */
@Service
class PublisherServiceImp implements PublisherService{

    @Autowired
    DauMapper dau;
    @Override
    public Long getDau(String date) {
        return dau.getDau(date);
    }

}
