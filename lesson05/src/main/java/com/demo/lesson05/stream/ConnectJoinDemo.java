package com.demo.lesson05.stream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectJoinDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 2. 读取数据--这里为了演示方便，就不监听端口，直接使用集合数据
        DataStreamSource<Tuple3<String,Double,Long>> server1Source = env.fromData(
                new Tuple3<>("server1", 2.3, 1L),  // 服务器id，cpu，时间
                new Tuple3<>("server2", 45.3, 1L),
                new Tuple3<>("server1", 32.1, 2L),
                new Tuple3<>("server2", 23.8, 2L)
                );
        DataStreamSource<Tuple4<String,Double,Double,Long>> server2Source = env.fromData(
                new Tuple4<>("server1", 50.9, 70.3, 1L), // 服务器id，内存，磁盘，时间
                new Tuple4<>("server2", 70.5, 48.9, 1L),
                new Tuple4<>("server1", 55.9, 70.5, 2L),
                new Tuple4<>("server2", 65.5, 49.5, 2L));
        // 3. 通过connect合并流，注意输出的是ConnectedStreams流
        ConnectedStreams<Tuple3<String, Double, Long>, Tuple4<String, Double, Double, Long>> connect = server1Source.connect(server2Source);
        // 4. keyBy-分组，保证相同的key在一个处理子任务重
        ConnectedStreams<Tuple3<String, Double, Long>, Tuple4<String, Double, Double, Long>> keyBy =
                connect.keyBy(value -> value.f0, value -> value.f0);
        // 5. 合并流进行关联操作
        SingleOutputStreamOperator<Tuple5<String, Double, Double, Double, Long>> map = keyBy.process(new CoProcessFunction<>() {

            // 存放第一条流数据暂时没匹配到的数据
            final Map<String, List<Tuple3<String, Double, Long>>> mapCache1 = new ConcurrentHashMap<>();
            // 存放第二条流数据暂时没匹配到的数据
            final Map<String, List<Tuple4<String, Double, Double, Long>>> mapCache2 = new ConcurrentHashMap<>();

            @Override
            public void processElement1(Tuple3<String, Double, Long> value, CoProcessFunction<Tuple3<String, Double, Long>, Tuple4<String, Double, Double, Long>, Tuple5<String, Double, Double, Double, Long>>.Context ctx, Collector<Tuple5<String, Double, Double, Double, Long>> out) throws Exception {
                // 匹配mapCache2
                Tuple4<String, Double, Double, Long> matchElement = null;
                if (mapCache2.get(value.f0) != null) {
                    for (Tuple4<String, Double, Double, Long> value2 : mapCache2.get(value.f0)) {
                        // 同一个服务器，并且采集时间相同，则匹配
                        if (value2.f0.equals(value.f0) && value2.f3.equals(value.f2)) {
                            out.collect(new Tuple5<>(value.f0, value.f1, value2.f1, value2.f2, value.f2));
                            matchElement = value2;
                            break;// 匹配到就退出
                        }
                    }
                    if(matchElement!=null){
                        // 匹配到就从缓存移除
                        mapCache2.get(value.f0).remove(matchElement);
                    }
                }
                // 没有匹配到，则加到mapCache1缓存中
                if(matchElement==null){
                    if (mapCache1.get(value.f0) == null) {
                        List<Tuple3<String, Double, Long>> list = new CopyOnWriteArrayList<>();
                        list.add(value);
                        mapCache1.put(value.f0, list);
                    } else {
                        mapCache1.get(value.f0).add(value);
                    }
                }
            }

            @Override
            public void processElement2(Tuple4<String, Double, Double, Long> value, CoProcessFunction<Tuple3<String, Double, Long>, Tuple4<String, Double, Double, Long>, Tuple5<String, Double, Double, Double, Long>>.Context ctx, Collector<Tuple5<String, Double, Double, Double, Long>> out) throws Exception {
                // 匹配mapCache1
                Tuple3<String, Double, Long> matchElement = null;
                if (mapCache1.get(value.f0) != null) {
                    for (Tuple3<String, Double, Long> value2 : mapCache1.get(value.f0)) {
                        // 同一个服务器，并且采集时间相同，则匹配
                        if (value2.f0.equals(value.f0) && value2.f2.equals(value.f3)) {
                            out.collect(new Tuple5<>(value.f0, value2.f1, value.f1, value.f2, value.f3));
                            matchElement = value2;// 匹配到就退出
                            break;
                        }
                    }
                    if(matchElement!=null){
                        // 匹配到就从缓存移除
                        mapCache1.get(value.f0).remove(matchElement);
                    }
                }
                // 没有匹配到，则加到mapCache2缓存中
                if(matchElement==null){
                    if (mapCache2.get(value.f0) == null) {
                        List<Tuple4<String, Double, Double, Long>> list = new CopyOnWriteArrayList<>();
                        list.add(value);
                        mapCache2.put(value.f0, list);
                    } else {
                        mapCache2.get(value.f0).add(value);
                    }
                }
            }

        });
        // 5. 输出
        map.print();
        // 执行
        env.execute();
    }
}
