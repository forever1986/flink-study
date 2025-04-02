package com.demo.lesson05.process;

import com.demo.lesson05.aggregate.KeyByOperatorDemo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcessFunctionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算
        KeyedStream<String, String> keyBy = text.keyBy(new KeyByOperatorDemo.KeySelectorFunction());
        // 4. process：自定义方法（只是简单的打印）
        SingleOutputStreamOperator<String> process = keyBy.process(new PrintProcessFunction());
        // 5. 输出-设置print算子的并行度为2
        process.print();
        // 执行
        env.execute();
    }

    // KeyedProcessFunction三个泛型分别是：key的类型，输入的类型，输出的类型
    public static class PrintProcessFunction extends KeyedProcessFunction<String, String, String> {

        /**
         * 我们需要关注一下几个参数
         * @param value 输入的值
         * @param ctx 上下文
         * @param out 输出的Collector，也就是输出到下一个算子
         * @throws Exception 抛出异常
         */
        @Override
        public void processElement(String value, KeyedProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
            System.out.println(value);// 简单做一下打印演示
            out.collect(value);
        }
    }

    public static class KeySelectorFunction implements KeySelector<String, String> {

        @Override
        public String getKey(String value) throws Exception {
            // 提取输入的输入
            String[] values = value.split(",");
            // 返回第一个值，作为keyBy的分类
            return values[0];
        }

    }
}
