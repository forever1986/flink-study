package com.demo.lesson03.environment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 创建不同执行环境以及配置
 */
public class StreamExecutionEnvironmentDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境：改动，使用createLocalEnvironmentWithWebUI，用于创建本地Web-UI控制台，需要引入flink-runtime-web依赖
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT,8999);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
//        env.setParallelism(); // 设置并行度
//        env.enableCheckpointing(); // 设置是否开启检查点
//        env.setRuntimeMode();  // 设置是批处理还是流处理，默认流处理
//        env.setDefaultSavepointDirectory(); // 设置保存点输出路径
//        env.disableOperatorChaining(); // 关闭合并算子链
//        env.socketTextStream(); // 设置socket数据源，在源算子中细讲
//        env.fromData();// 设置集合数据源，在源算子中细讲
//        env.fromSource();// 设置数据源，在源算子中细讲

        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);
        // 3. 输出
        text.print();

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
