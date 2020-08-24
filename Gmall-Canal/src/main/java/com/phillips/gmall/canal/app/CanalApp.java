package com.phillips.gmall.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

public class CanalApp {

    public static void main(String[] args) {
        // 1.连接canal服务器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        // 2.抓取数据
        while (true) {
            // 连接
            canalConnector.connect();
            // 订阅该数据库中所有的表
            canalConnector.subscribe("gmall_realtime.*");
            // 一个Message代表一次抓取，一次抓取可以抓多个sql的执行结果集
            Message message = canalConnector.get(100);

            if (message.getEntries().size() == 0) {
                System.out.println("没有数据，休息5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // 3.抓取数据，提取数据
                // 一个entry代表一个sql执行的结果集
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // 其中有时会有事务开启关闭等操作，不需要，通过if进行过滤
                    if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                        // 业务数据
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            // 序列化工具
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // 表名
                        String tableName = entry.getHeader().getTableName();

                        CanalHandler canalHandler = new CanalHandler(rowChange.getEventType(), tableName, rowDatasList);
                        // 4.处理业务数据，发送 kafka 到对应的 topic
                        canalHandler.handle();
                    }
                }
            }
        }


    }
}
