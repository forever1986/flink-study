package com.demo.lesson05.process;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 假设是监听来自多个服务器cpu的日志，日志格式有2种，一种是“服务器id,cpu,时间”，一种是“服务器id,cpu,内存,磁盘容量,时间”，我们合并成一条流，只取其中“服务器id,cpu,时间”,并打印
 */
public class CoProcessFunctionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据--这里为了演示方便，就不监听端口，直接使用集合数据
        DataStreamSource<Tuple3<String,Double,Long>> server1Source = env.fromData(
                new Tuple3<>("server1", 2.3, 1L),
                new Tuple3<>("server1", 45.3, 2L));
        DataStreamSource<Tuple5<String,Double,Double,Double,Long>> server2Source = env.fromData(
                new Tuple5<>("server2", 6.9, 50.9, 80.5, 1L),
                new Tuple5<>("server2", 33.2, 70.5, 67.4, 2L));
        // 3. 通过union合并流，注意输出的是ConnectedStreams流
        ConnectedStreams<Tuple3<String, Double, Long>, Tuple5<String, Double, Double, Double, Long>> connect = server1Source.connect(server2Source);
        connect.keyBy(
                new KeySelector<Tuple3<String, Double, Long>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Double, Long> value) throws Exception {
                        return "";
                    }
                },
                new KeySelector<Tuple5<String, Double, Double, Double, Long>, String>() {

                    @Override
                    public String getKey(Tuple5<String, Double, Double, Double, Long> value) throws Exception {
                        return "";
                    }
                });

        connect.process(new CoProcessFunction<Tuple3<String, Double, Long>, Tuple5<String, Double, Double, Double, Long>, String>() {
            @Override
            public void processElement1(Tuple3<String, Double, Long> value, CoProcessFunction<Tuple3<String, Double, Long>, Tuple5<String, Double, Double, Double, Long>, String>.Context ctx, Collector<String> out) throws Exception {

            }

            @Override
            public void processElement2(Tuple5<String, Double, Double, Double, Long> value, CoProcessFunction<Tuple3<String, Double, Long>, Tuple5<String, Double, Double, Double, Long>, String>.Context ctx, Collector<String> out) throws Exception {

            }
        });
        /**
         * 4. 我们对ConnectedStreams进行map操作，需要实现的是CoMapFunction函数，这个函数与我们之前的MapFunction有区别
         *    1）CoMapFunction函数是一个需要3个泛型以及实现2个方法的函数，第一个和第二个泛型分别是第一个流和第二个流的数据类型，第三个泛型是输出到下游算子的类型
         *    2) CoMapFunction函数输出的是一个DataStream,意味着两个流最终会合并为一个流
         *    3）CoMapFunction里面需要实现2个map方法，也就是说2种数据类型分别处理
         */
        SingleOutputStreamOperator<Tuple3<String, Double, Long>> map = connect.map(new CoMapFunction<>() {

            /**
             * map1函数是处理来自server1Source的数据
             */
            @Override
            public Tuple3<String, Double, Long> map1(Tuple3<String, Double, Long> value) throws Exception {
                // 由于server1服务器的格式符合最终格式，直接返回
                return value;
            }

            /**
             * map1函数是处理来自server2Source的数据
             */
            @Override
            public Tuple3<String, Double, Long> map2(Tuple5<String, Double, Double, Double, Long> value) throws Exception {
                // 由于server2服务器的格式不符合最终格式，我们转换后再返回
                return new Tuple3<>(value.f0, value.f1, value.f4);
            }
        });
        // 5. 输出
        map.print().setParallelism(1);
        // 执行
        env.execute();
    }
}
