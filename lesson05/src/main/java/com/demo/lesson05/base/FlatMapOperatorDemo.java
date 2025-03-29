package com.demo.lesson05.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 假设是一个监听来自服务器cpu的日志，日志格式是“服务器id,cpu,时间”，并且可能多条一起，以；分号隔离
 * 将其转换为一个或多个三元组，并输出
 */
public class FlatMapOperatorDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算
        DataStream<Tuple3<String,Double,Long>> flatMap = text.flatMap(new Tuple3FlatMapFunction());
        // 4. 输出
        flatMap.print();
        // 执行
        env.execute();
    }


    public static class Tuple3FlatMapFunction implements FlatMapFunction<String, Tuple3<String,Double,Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple3<String, Double, Long>> collector) throws Exception {
            String[] datas = value.split(";");
            for (String data : datas) {
                String[] values = data.split(",");
                String value1 = values[0];
                double value2 = Double.parseDouble("0");
                long value3 = 0;
                if (values.length >= 2) {
                    try {
                        value2 = Double.parseDouble(values[1]);
                    } catch (Exception e) {
                        value2 = Double.parseDouble("0");
                    }
                }
                if (values.length >= 3) {
                    try {
                        value3 = Long.parseLong(values[2]);
                    } catch (Exception ignored) {
                    }
                }
                collector.collect(new Tuple3<>(value1, value2, value3));
            }
        }
    }

}
