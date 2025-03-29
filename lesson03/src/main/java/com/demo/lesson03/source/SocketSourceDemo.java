package com.demo.lesson03.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 来自socket的数据
 */
public class SocketSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据-来自socket
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        // 3. 输出
        source.print();
        // 执行
        env.execute();
    }

}
