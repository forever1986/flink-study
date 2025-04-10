package com.demo.lesson09.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class JoinDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 2. 读取数据--这里为了演示方便，就不监听端口，直接使用集合数据
        KeyedStream<Tuple3<String, Double, Long>, String> server1Source = env.fromData(
                        new Tuple3<>("server1", 2.3, 1L),  // 服务器id，cpu，时间
                        new Tuple3<>("server2", 45.3, 1L),
                        new Tuple3<>("server1", 32.1, 11L),
                        new Tuple3<>("server2", 23.8, 5L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Double, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) ->{
                            System.out.println("event time = "+element.f2+ " 初始时间戳" + recordTimestamp);
                            return element.f2 * 1000L;
                        }))
                .keyBy(value -> value.f0);
        KeyedStream<Tuple4<String, Double, Double, Long>, String> server2Source = env.fromData(
                        new Tuple4<>("server1", 50.9, 70.3, 5L), // 服务器id，内存，磁盘，时间
                        new Tuple4<>("server2", 70.5, 48.9, 2L),
                        new Tuple4<>("server1", 55.9, 70.5, 14L),
                        new Tuple4<>("server2", 65.5, 49.5, 8L))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple4<String, Double, Double, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) ->{
                            System.out.println("event time = "+element.f3+ " 初始时间戳" + recordTimestamp);
                            return element.f3 * 1000L;
                        }))
                .keyBy(value -> value.f0);
        // 3. 通过join合并流
        DataStream<Tuple5<String, Double, Double, Double, Long>> apply = server1Source.join(server2Source)
                // cpu数据量的key 等于 内存数据流的key
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                // 开窗，使用滚动窗口，5s
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                // apply当数据匹配上（窗口一直，且key一致的数据会到apply方法中）
                .apply(new JoinFunction<>() {
                    @Override
                    public Tuple5<String, Double, Double, Double, Long> join(Tuple3<String, Double, Long> first, Tuple4<String, Double, Double, Long> second) throws Exception {
                        System.out.println("first=" + first + " -> second=" + second);
                        return new Tuple5<>(first.f0, first.f1, second.f1, second.f2, first.f2);
                    }
                });
        // 4. 输出
        apply.print();
        // 执行
        env.execute();
    }
}
