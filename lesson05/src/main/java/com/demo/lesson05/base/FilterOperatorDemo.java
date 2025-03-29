package com.demo.lesson05.base;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 假设是一个监听来自服务器cpu的日志，日志格式是“服务器id,cpu,时间”
 * 将过滤服务器id=server1的数据，并输出
 */
public class FilterOperatorDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算
        DataStream<String> filter = text.filter(new FilterFunctionDemo());
        // 4. 输出
        filter.print();
        // 执行
        env.execute();
    }


    public static class FilterFunctionDemo implements FilterFunction<String> {

        @Override
        public boolean filter(String value) throws Exception {
            String[] values = value.split(",");
            return "server1".equals(values[0]);
        }
    }

}
