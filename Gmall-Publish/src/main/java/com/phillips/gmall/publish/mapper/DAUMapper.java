package com.phillips.gmall.publish.mapper;

public interface DAUMapper {

    /**
     * 查询某日用户活跃总数
     */
    Long getDauTotal(String date);
}
