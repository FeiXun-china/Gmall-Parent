package com.phillips.gmall.publish.service.impl;

import com.phillips.gmall.publish.mapper.DAUMapper;
import com.phillips.gmall.publish.service.PublishService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PublishServiceImpl implements PublishService {

    @Autowired
    DAUMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }
}
