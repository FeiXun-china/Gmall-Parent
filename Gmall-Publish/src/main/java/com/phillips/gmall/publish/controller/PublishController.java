package com.phillips.gmall.publish.controller;

import com.alibaba.fastjson.JSON;
import com.phillips.gmall.publish.service.PublishService;
import org.apache.avro.data.Json;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublishController {

    @Autowired
    PublishService publishService;

    // http://publisher:8070/realtime-total?date=2019-02-01
    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String dateString) {
        Long dauTotal = publishService.getDauTotal(dateString);
        List<Map> list = new ArrayList<Map>();
        // 日活总数
        Map dauMap=new HashMap<String,Object>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        list.add(dauMap);

        return JSON.toJSONString(list);
    }


}
