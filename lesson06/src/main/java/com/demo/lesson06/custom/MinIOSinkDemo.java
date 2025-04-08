package com.demo.lesson06.custom;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MinIOSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 假设输入数据为字符串流
        DataStream<String> dataStream = env.fromElements("Hello", "MinIO", "Flink");

        // 配置MinIO连接参数
        String endpoint = "http://127.0.0.1:9005/";
        String accessKey = "minioadmin";
        String secretKey = "minioadmin";
        String bucket = "flink-data";

        // 添加自定义Sink
        dataStream.sinkTo(new MinIOSink(endpoint, accessKey, secretKey, bucket));

        env.execute("Write to MinIO Example");
    }
}
