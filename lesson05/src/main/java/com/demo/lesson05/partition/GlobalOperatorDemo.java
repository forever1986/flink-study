package com.demo.lesson05.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将输入的数据，全局分区分配到下游print算子打印
 */
public class GlobalOperatorDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);//为了演示分区，需要设置一个多并行度才能看出效果，这里设置最简单的2来做示例
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算并全局分区分配到输出print算子，所谓全局就是只分配到第一个下游的子任务
        text.global().print();
        // 执行
        env.execute();
    }

}
