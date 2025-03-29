package com.demo.lesson03.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 来自kafka数据
 */
public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据-来自kafka数据，注意这里使用的是Builder编码风格
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("clickhouse1:9092,clickhouse2:9092,clickhouse3:9092") //kafka的地址
                .setGroupId("test") // 消费分组id
                .setTopics("mytopic") // 消费的Topics
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 值的反序列化，这里使用默认String
                /**
                 * 注意这里的消费策略有earliest和latest，但是和kafka消费者的earliest和latest是有所区别的
                 * 1）kafka消费者本身会先判断是否有offset，如果有则从offset开始
                 * 2）flink的并不会判断offsets
                 */
                .setStartingOffsets(OffsetsInitializer.latest()) // 消费策略
                .build();
        // 注意，这里有一个WatermarkStrategy，是一个水位线，这个我们讲到水位线时再讲
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        // 3. 输出
        source.print();
        // 执行
        env.execute();
    }

}
