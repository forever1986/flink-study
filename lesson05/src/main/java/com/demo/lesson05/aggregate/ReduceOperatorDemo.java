package com.demo.lesson05.aggregate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 假设是一个监听来自服务器cpu的日志，日志格式是“服务器id,cpu,时间”
 * 将通过服务器id对数据进行keyBy分组，通过参数设置计算其cpu的max值或min值，其它属性取值也是根据参数设置
 */
public class ReduceOperatorDemo {

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
        // 使用参数设置，求最大值，并且其它非聚合属性取最大值那一条数据
        keyBy.reduce(new Tuple3ReduceFunction(true,true)).print();
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

    public static class Tuple3ReduceFunction implements ReduceFunction<Tuple3<String,Double,Long>> {

        boolean isMax;// 如果是true，则取最大值，反之取最小值
        boolean isBy;// 如果是true，则每次其它非聚合属性都取被选中的记录

        public Tuple3ReduceFunction(boolean isMax, boolean isBy) {
            this.isMax = isMax;
            this.isBy = isBy;
        }

        @Override
        public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> value1, Tuple3<String, Double, Long> value2) throws Exception {
            System.out.println("==========执行reduce==========");
            if(isMax){
                if(value1.f1 < value2.f1){
                    if(isBy){
                        return value2;
                    }else{
                        Tuple3<String, Double, Long> returnValue = value1.copy();
                        returnValue.f1 = value2.f1;
                        return returnValue;
                    }
                }
            }else{
                if(value1.f1 > value2.f1){
                    if(isBy){
                        return value2;
                    }else{
                        Tuple3<String, Double, Long> returnValue = value1.copy();
                        returnValue.f1 = value2.f1;
                        return returnValue;
                    }
                }
            }
            return value1.copy();
        }
    }

}
