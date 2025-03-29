package com.demo.lesson05.stream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 假设是监听来自多个服务器cpu的日志，日志格式是“服务器id,cpu,时间”，数据来自多台服务器的数据，我们合并成一条流并打印
 */
public class UnionStreamDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据--这里为了演示方便，就不监听端口，直接使用集合数据
        DataStreamSource<Tuple3<String,Double,Long>> server1Source = env.fromData(
                new Tuple3<>("server1", 2.3, 1L),
                new Tuple3<>("server1", 45.3, 2L));
        DataStreamSource<Tuple3<String,Double,Long>> server2Source = env.fromData(
                new Tuple3<>("server2", 6.9, 1L),
                new Tuple3<>("server2", 33.2, 2L));
        DataStreamSource<Tuple3<String,Double,Long>> server3Source = env.fromData(
                new Tuple3<>("server3", 78.7, 1L),
                new Tuple3<>("server3", 6.9, 2L));
        // 3. 通过union合并流
        DataStream<Tuple3<String, Double, Long>> union = server1Source.union(server2Source, server3Source);
        // 4. 输出
        union.print().setParallelism(1);
        // 执行
        env.execute();
    }
}
