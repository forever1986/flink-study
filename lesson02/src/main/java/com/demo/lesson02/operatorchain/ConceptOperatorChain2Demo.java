package com.demo.lesson02.operatorchain;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 算子链演示示例2
 */
public class ConceptOperatorChain2Demo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
//        env.disableOperatorChaining();// 设置全局不合并算子链
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);
        // 3. 计算
        text.map(String::toLowerCase).name("mymap") // 做一次map
                .startNewChain() // 开启新链条
                .flatMap(new Tokenizer()).name("myflatmap") // 做一次flatMap
//                .disableChaining() //  不做前后合并算子链
                .print().name("myprint"); //做一次print

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
