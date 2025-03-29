package com.demo.lesson02.slot;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 算子演示示例
 */
public class ConceptSlotDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        Configuration configuration = new Configuration();
        configuration.set()
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);
        // 3. 计算
        DataStream<Tuple2<String, Integer>> counts =
                // 给flatMap命名为myflatmap
                text.slotSharingGroup("firstGroup")
                        .flatMap(new Tokenizer()).name("myflatmap").slotSharingGroup("firstGroup")
                        .keyBy(value -> value.f0)
                        // 给sum命名为mysum
                        .sum(1).name("mysum").slotSharingGroup("lastGroup");
        // 4. 输出：给print命名为myprint
        counts.print().name("myprint").slotSharingGroup("lastGroup");

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
