package com.demo.lesson06.custom2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MinIOSinkDemo {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///checkpoint");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(5000); // 启用检查点
        env.setParallelism(1);
        // 假设输入数据为字符串流
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<Tuple2<String, String>> map = source.map(new Tuple2MapFunction());

        // 配置MinIO连接参数
        String endpoint = "http://127.0.0.1:9005/";
        String accessKey = "minioadmin";
        String secretKey = "minioadmin";
        String bucket = "flink-data";

        // 添加自定义Sink
        map.sinkTo(new MinIOSink(endpoint, accessKey, secretKey, bucket));
        env.execute("Write to MinIO Example");
    }

    public static class Tuple2MapFunction implements MapFunction<String, Tuple2<String,String>> {

        @Override
        public Tuple2<String, String> map(String value) throws Exception {
            String[] values = value.split(",");
            String value1 = values[0];
            String value2 = "";
            long value3 = 0;
            if(values.length >= 2){
                value2 = values[1];
            }
            return new Tuple2<>(value1,value2);
        }
    }
}
