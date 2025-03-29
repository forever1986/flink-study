package com.demo.lesson05.base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 假设是一个监听来自服务器cpu的日志，日志格式是“服务器id,cpu,时间”
 * 将其转换为一个三元组，并输出
 */
public class MapOperatorDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算
        DataStream<Tuple3<String,Double,Long>> map = text.map(new Tuple3MapFunction());
        // 4. 输出
        map.print();
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

}
