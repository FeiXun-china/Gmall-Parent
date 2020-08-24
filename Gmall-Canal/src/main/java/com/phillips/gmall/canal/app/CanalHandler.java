package com.phillips.gmall.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.phillips.gmall.common.constants.GmallConstant;

import java.util.List;

public class CanalHandler {


    CanalEntry.EventType eventType; // 类型 update insert
    String tableName; // 表名
    List<CanalEntry.RowData> rowDataList; // 结果集

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        //下单操作
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT == eventType) {
            rowDateList2Kafka(GmallConstant.KAFKA_TOPIC_ORDER);
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT == eventType || CanalEntry.EventType.UPDATE == eventType)) {
            rowDateList2Kafka(GmallConstant.KAFKA_TOPIC_USER);
        }

    }

    private void rowDateList2Kafka(String kafkaTopic) {
        for (CanalEntry.RowData rowData : rowDataList) {
            // getAfterColumnsList 数据发生变化以后的 针对插入，就没有getBeforeColumnsList方法，因为插入之前就没有数据
            // getBeforeColumnsList 数据发生变化之前的
            List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : columnsList) {
//                System.out.println(column.getName() + "::::" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }

            MyKafkaSender.send(kafkaTopic, jsonObject.toJSONString());
        }

    }

}
