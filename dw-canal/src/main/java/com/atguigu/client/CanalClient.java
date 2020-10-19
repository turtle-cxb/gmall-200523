package com.atguigu.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstant;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.sql.rowset.RowSetMetaDataImpl;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) {
        //创建canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop107", 11111),
                "example",
                "",
                ""
        );

        while (true){
            //连接mysql并获取数据
            canalConnector.connect();
            canalConnector.subscribe("gmall200523.*");
            Message message = canalConnector.get(100);


            if (message.getEntries().size()<=0){
                System.out.println("没有数据，休息一下！！！！");
                System.out.println("============");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {


                List<CanalEntry.Entry> entries = message.getEntries();
                for (CanalEntry.Entry entry : entries) {
                    //判断EntryType是否是ROWDATA类型，不是就不用解析
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){

                        try {

                            //获取表名
                            String tableName = entry.getHeader().getTableName();
                            //获取RowChange
                            ByteString storeValue = entry.getStoreValue();
                            //获取EventType(对数据的操作是新增还是修改)RowDataList(结果数据的集合)
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            handler(tableName,eventType,rowDatasList);



                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }


                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if(tableName.equals("order_info") && eventType.equals(CanalEntry.EventType.INSERT)){
            rowDataSender(rowDatasList,GmallConstant.KAFKA_TOPIC_ORDER_INFO);

        }else if(tableName.equals("user_info") && (eventType.equals(CanalEntry.EventType.INSERT ) || (eventType.equals(CanalEntry.EventType.UPDATE)))){
            rowDataSender(rowDatasList,GmallConstant.KAFKA_TOPIC_USER_INFO);

        } else if("order_detail".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT)){
            rowDataSender(rowDatasList,GmallConstant.KAFKA_TOPIC_ORDER_DETAIL_INFO);
            try {
                Thread.sleep(new Random().nextInt(5)*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void rowDataSender(List<CanalEntry.RowData> rowDatasList,String topic){
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(),column.getValue());
            }
            System.out.println(jsonObject);
            MyKafkaSender.send(topic,jsonObject.toJSONString());
        }

    }





}
