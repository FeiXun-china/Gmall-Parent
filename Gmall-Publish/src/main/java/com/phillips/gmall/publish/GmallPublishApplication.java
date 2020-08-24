package com.phillips.gmall.publish;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.phillips.gmall.publish.mapper")
public class GmallPublishApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublishApplication.class, args);
    }

}
