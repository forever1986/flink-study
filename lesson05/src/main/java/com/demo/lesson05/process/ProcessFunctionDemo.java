package com.demo.lesson05.process;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


/**
 * 使用自定义的方式实现一个类似map功能，只不过输入和输出类型都是固定的
 * 将输入的字符串转换为Tuple3ProcessFunction
 */
public class ProcessFunctionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算: DataStream流的process方法入参是一个ProcessFunction
        DataStream<Tuple3<String,Double,Long>> map = text.process(new Tuple3ProcessFunction());
        // 4. 输出
        map.print();
        // 执行
        env.execute();
    }


    // 自定义的ProcessFunction
    public static class Tuple3ProcessFunction extends ProcessFunction<String, Tuple3<String, Double, Long>> {

        /**
         * 我们需要关注一下几个参数
         * @param value 输入的值
         * @param ctx 上下文
         * @param out 输出的Collector，也就是输出到下一个算子
         * @throws Exception 抛出异常
         */
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple3<String,Double,Long>> out) throws Exception {
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
            out.collect(new Tuple3<>(value1,value2,value3));
        }
    }
}
