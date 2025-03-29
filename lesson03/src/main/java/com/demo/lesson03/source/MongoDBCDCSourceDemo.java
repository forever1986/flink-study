package com.demo.lesson03.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MongoDBCDCSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据-来自mongoDB的数据，注意这里使用的是Builder编码风格
        MongoDBSource<String> mongoDBSource = MongoDBSource.<String>builder()
                // 副本集连接方式，副本集名称myrs0
                .hosts("192.168.2.104:27017,192.168.2.105:27017,192.168.2.106:27017/?replicaSet=myrs0")
                // 数据库
                .databaseList("test")
                // 集合（一定要带上数据库名称）
                .collectionList("test.test")
                // 数据转换，这里使用现成转换JSON
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        // 注意，这里有一个WatermarkStrategy，是一个水位线，这个我们讲到水位线时再讲
        DataStreamSource<String> source = env.fromSource(mongoDBSource, WatermarkStrategy.noWatermarks(), "mongoDBSource").setParallelism(1);
        // 3. 输出
        source.print().setParallelism(1);
        // 执行
        env.execute();
    }
}
