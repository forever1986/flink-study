package com.demo.lesson06;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 需要设置checkpoint（精准一次必须设置）
        env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);
        // 2. 读取数据-来自socket
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        // 3. 输出：
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // kafka的地址
                .setBootstrapServers("clickhouse1:9092,clickhouse2:9092,clickhouse3:9092")
                // 配置序列化类和topic
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("mytopic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // 指定事务前缀（精准一次必须设置）
                .setTransactionalIdPrefix("flink-")
                // 指定使用精准一次（精准一次必须设置）
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 指定事务超时时间（精准一次必须设置）：该时间必须大于checkpoint时间
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, ((Integer)(3 * 60 * 1000)).toString())
                .build();
        source.sinkTo(kafkaSink);
        // 执行
        env.execute();
    }
}
