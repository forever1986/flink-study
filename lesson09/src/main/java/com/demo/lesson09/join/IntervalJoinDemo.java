package com.demo.lesson09.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class IntervalJoinDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 读取数据--这里为了演示方便，就不监听端口，直接使用集合数据
        KeyedStream<Tuple3<String, Double, Long>, String> server1Source = env
                .socketTextStream("localhost", 8888)
                .map(new Tuple3MapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Double, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) ->{
                            System.out.println("event time = "+element.f2+ " 初始时间戳" + recordTimestamp);
                            return element.f2 * 1000L;
                        }))
                .keyBy(value -> value.f0);
        KeyedStream<Tuple4<String, Double, Double, Long>, String> server2Source = env
                .socketTextStream("localhost", 9999)
                .map(new Tuple4MapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple4<String, Double, Double, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) ->{
                            System.out.println("event time = "+element.f3+ " 初始时间戳" + recordTimestamp);
                            return element.f3 * 1000L;
                        }))
                .keyBy(value -> value.f0);
        OutputTag<Tuple3<String,Double,Long>> leftOutputTag = new OutputTag<>("leftOutputTag", Types.TUPLE(Types.STRING,Types.DOUBLE,Types.LONG)); // cpu流关闭窗口之后的数据
        OutputTag<Tuple4<String,Double,Double,Long>> rightOutputTag = new OutputTag<>("rightOutputTag", Types.TUPLE(Types.STRING,Types.DOUBLE,Types.DOUBLE,Types.LONG)); // 内存流关闭窗口之后的数据
        // 3. 通过join合并流
        SingleOutputStreamOperator<Tuple5<String, Double, Double, Double, Long>> process = server1Source.intervalJoin(server2Source)
                // 设置intervalJoin的上下界，分别是-2和2，意味着如果事件时间为3，则可以匹配到1-5的数据
                .between(Duration.ofSeconds(-2), Duration.ofSeconds(2))
                // 设置左流-cpu流关闭窗口之后的数据
                .sideOutputLeftLateData(leftOutputTag)
                // 设置右流-内存流关闭窗口之后的数据
                .sideOutputRightLateData(rightOutputTag)
                // 当两条流数据匹配了，且窗口还未关闭时，会调用process方法
                .process(new ProcessJoinFunction<>() {
                    @Override
                    public void processElement(
                            Tuple3<String, Double, Long> left,
                            Tuple4<String, Double, Double, Long> right,
                            ProcessJoinFunction<Tuple3<String, Double, Long>, Tuple4<String, Double, Double, Long>, Tuple5<String, Double, Double, Double, Long>>.Context ctx,
                            Collector<Tuple5<String, Double, Double, Double, Long>> out) throws Exception {
                        System.out.println("left=" + left + " -> right=" + right);
                        out.collect(new Tuple5<>(left.f0, left.f1, right.f1, right.f2, left.f2));
                    }
                });
        // 4. 输出
        process.print("主流");
        process.getSideOutput(leftOutputTag).print("左流关闭窗口之后的数据：");
        process.getSideOutput(rightOutputTag).print("右流关闭窗口之后的数据：");
        // 执行
        env.execute();
    }

    public static class Tuple3MapFunction implements MapFunction<String, Tuple3<String,Double,Long>> {

        @Override
        public Tuple3<String, Double, Long> map(String value) throws Exception {
            String[] values = value.split(",");
            String value1 = values[0];
            double value2 = Double.parseDouble("0");
            long value3 = 0;
            if(values.length >= 2){
                try {
                    value2 = Double.parseDouble(values[1]);
                }catch (Exception e){
                    value2 = Double.parseDouble("0");
                }
            }
            if(values.length >= 3){
                try {
                    value3 = Long.parseLong(values[2]);
                }catch (Exception ignored){
                }
            }
            return new Tuple3<>(value1,value2,value3);
        }
    }

    public static class Tuple4MapFunction implements MapFunction<String, Tuple4<String,Double,Double,Long>> {

        @Override
        public Tuple4<String,Double,Double,Long> map(String value) throws Exception {
            String[] values = value.split(",");
            String value1 = values[0];
            double value2 = Double.parseDouble("0");
            double value3 = Double.parseDouble("0");
            long value4 = 0;
            if(values.length >= 2){
                try {
                    value2 = Double.parseDouble(values[1]);
                }catch (Exception e){
                    value2 = Double.parseDouble("0");
                }
            }
            if(values.length >= 2){
                try {
                    value3 = Double.parseDouble(values[2]);
                }catch (Exception e){
                    value3 = Double.parseDouble("0");
                }
            }
            if(values.length >= 3){
                try {
                    value4 = Long.parseLong(values[3]);
                }catch (Exception ignored){
                }
            }
            return new Tuple4<>(value1,value2,value3,value4);
        }
    }
}
