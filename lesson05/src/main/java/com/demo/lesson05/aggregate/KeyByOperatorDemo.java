package com.demo.lesson05.aggregate;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 假设是一个监听来自服务器cpu的日志，日志格式是“服务器id,cpu,时间”
 * 将通过服务器id对数据进行keyBy分组，并输出
 */
public class KeyByOperatorDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算
        KeyedStream<String, String> keyBy = text.keyBy(new KeySelectorFunction());
        // 4. 输出-设置print算子的并行度为2
        keyBy.print().setParallelism(2);
        // 执行
        env.execute();
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
