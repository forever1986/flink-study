package com.demo.lesson03.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 来自集合的数据
 */
public class CollectionSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据-来自集合
        DataStreamSource<Integer> source = env.fromData(Arrays.asList(1, 22, 36, 54));
//        env.fromCollection(); // 已经废弃，其底层使用addSource方式，这个已经被抛弃。新的是fromSource方法，fromData底层就是使用fromSource
//        env.fromElements(); // 已经废弃，其底层使用addSource方式，这个已经被抛弃。新的是fromSource方法，fromData底层就是使用fromSource
        // 3. 输出
        source.print();
        // 执行
        env.execute();
    }
}
