package com.demo.lesson02;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WebUIDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境：改动，使用createLocalEnvironmentWithWebUI，用于创建本地Web-UI控制台，需要引入flink-runtime-web依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);
        // 3. 计算
        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer()) // 3.1 切分：按照空格或标点切分每一句中的词，并输出（词，数量）的元组
                        .keyBy(value -> value.f0)  // 3.2 分组：这里的f0代表二元组第一个元素，也就是按照词进行分组，分组只是分到不同并行度，并不会改变输出值
                        .sum(1); // 3.3 聚合：分组之后分别计算，这里的1代表二元组的第二个元素，也就是按照第二个元素进行累积
        // 4. 输出
        counts.print();

        // 执行
        env.execute();
    }

    /**
     * 自定义切分句子的分词器，遇到空格、标点符合都是切割，并输出的是一个Tuple2
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (!token.isEmpty()) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
