package com.demo.lesson05.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 假设是一个监听来自服务器cpu的日志，日志格式是“服务器id,cpu,时间”,我们按照服务器id的hash值作为key，去分配到下游算子
 */
public class CustomPartitionOperatorDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);//为了演示分区，需要设置一个多并行度才能看出效果，这里设置最简单的2来做示例
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 按照服务器id的hash值作为key，去分配到下游算子
        text.partitionCustom(new CustomPartitioner(),new CustomKeySelector()).print();
        // 执行
        env.execute();
    }

    // 需要一个泛型，是key的格式
    public static class CustomPartitioner implements Partitioner<String> {

        @Override
        public int partition(String key, int numPartitions) {
            // 这里我们可以使用key来分配，也可以不选择key来分配，看你的业务需求
            // 示例要求我们需要通过服务器id来区分，那么我们需要使用key
            return key.hashCode() % numPartitions;
        }
    }

    // 第一个泛型是数据的格式，第二个泛型是key的格式
    public static class CustomKeySelector implements KeySelector<String, String> {
        @Override
        public String getKey(String value) throws Exception {
            // 需求要求我们按照服务器id来分区，那么我们需要把服务器id作为key返回
            String[] values = value.split(",");
            return values[0];
        }
    }

}
