package com.demo.lesson05.aggregate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 假设是一个监听来自服务器cpu的日志，日志格式是“服务器id,cpu,时间”
 * 将通过服务器id对数据进行keyBy分组，计算其cpu的max值，min值，以及sum总量
 */
public class SumMaxMinOperatorDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 这里为了方便说明结果，将并行度设置为1
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算并输出
        KeyedStream<Tuple3<String, Double, Long>, String> keyBy = text
                .map(new Tuple3MapFunction())
                .keyBy(new KeySelectorFunction());
        // 这里面传入1，是代表取Tuple3的第二个值来做聚合（注意：如果是自定义的pojo，则需要使用字符串的属性名称）
        keyBy.sum(1).print("sum");
        keyBy.max(1).print("min");
        keyBy.min(1).print("max");
        // 执行
        env.execute();
    }


    public static class KeySelectorFunction implements KeySelector<Tuple3<String,Double,Long>, String> {

        @Override
        public String getKey(Tuple3<String,Double,Long> value) throws Exception {
            // 返回第一个值，作为keyBy的分类
            return value.f0;
        }

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
