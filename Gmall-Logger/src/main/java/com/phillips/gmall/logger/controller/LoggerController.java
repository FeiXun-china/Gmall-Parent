package com.phillips.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.phillips.gmall.common.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController // 相当于@Controller + @ResponseBody
//@Controller
@Slf4j // 在编译的时候自动生成log对象，可以使用log.debug(logString)
public class LoggerController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/log")
//    @ResponseBody
    public String log(@RequestParam("logString") String logString) {

        log.info(logString);

        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        // 将日志发送到kafka
        String type = (String) jsonObject.get("type");
        if ("startup".equals(type)) {
            kafkaTemplate.send(GmallConstant.KAFKA_STARTUP, jsonObject.toJSONString());
        } else {
            kafkaTemplate.send(GmallConstant.KAFKA_EVENT, jsonObject.toJSONString());
        }
        return "success";
    }

}
