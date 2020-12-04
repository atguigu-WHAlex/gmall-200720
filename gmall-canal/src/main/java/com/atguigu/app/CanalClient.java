package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstant;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) throws InvalidProtocolBufferException {

        //1.获取Canal的连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");

        //2.读取数据并将数据解析
        while (true) {

            //3.连接机器
            canalConnector.connect();

            //4.订阅库
            canalConnector.subscribe("gmall200720.*");

            //5.抓取数据
            Message message = canalConnector.get(100);

            //6.获取Entry数据
            List<CanalEntry.Entry> entries = message.getEntries();

            //7.判断当前抓取的数据是否为空
            if (entries.size() <= 0) {
                //当前抓取没有数据
                System.out.println("当前抓取没有数据,休息一会！！！");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                //当前抓取有数据,解析Entry
                for (CanalEntry.Entry entry : entries) {

                    //判断Entry类型,只需要RowData类型
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

                        //a.获取表名
                        String tableName = entry.getHeader().getTableName();

                        //b.获取内部数据
                        ByteString storeValue = entry.getStoreValue();

                        //c.将数据反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //d.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //e.获取行改变的记录信息
                        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

                        //f.根据表名以及事件类型处理数据集
                        handler(tableName, eventType, rowDataList);
                    }
                }
            }
        }
    }

    //根据表名以及事件类型处理数据集,将数据发送至Kafka
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {

        //选择OrderInfo表,只需要下单数据(Insert)
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            //解析rowDataList
            for (CanalEntry.RowData rowData : rowDataList) {

                //创建JSON对象,用于存放多个列的数据
                JSONObject jsonObject = new JSONObject();

                //获取新增之后的数据列集合,并遍历
                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }

                //打印并将数据写入Kafka
                String msg = jsonObject.toString();
                System.out.println(msg);
                MyKafkaSender.send(GmallConstant.GMALL_ORDER_INFO_TOPIC, msg);
            }
        }
    }
}
