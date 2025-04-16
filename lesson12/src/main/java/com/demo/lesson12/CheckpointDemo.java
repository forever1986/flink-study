package com.demo.lesson12;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CheckpointDemo {

    public static void main(String[] args) throws Exception {

        // 1）设置检查点的存储方式和路径
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");// 存储路径是Hadoop的话，需要引入Hadoop依赖
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///home/linmoo/Downloads/flink-1.19.2/checkpoints"); // 这里演示在默认本地存储
        // 2）配置保留几份checkpoint
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 3);
        // 3）创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // 4）每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(10000);
        // 5）开启增量保存，除了配置该设置，其它的需要在conf.yaml文杰中配置，包括存储路径等信息
//        env.enableChangelogStateBackend(true);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 6）设置模式为精确一次 (这是默认值)
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 7）Checkpoint 必须在一分钟内完成，否则就会被抛弃
        checkpointConfig.setCheckpointTimeout(60000);
        // 8）允许两个连续的 checkpoint 错误
        checkpointConfig.setTolerableCheckpointFailureNumber(2);
        // 9）同一时间只允许一个 checkpoint 进行
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 10）上一个 checkpoints 完成之后，间隔 500 ms 之后在发下一个checkpoints
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 11）使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 12）使用非Barrier 对齐（Barrier Alignment）-- setCheckpointingMode必须是CheckpointingMode.EXACTLY_ONCE，否则该设置无效
//        checkpointConfig.enableUnalignedCheckpoints();
        // 13）开启非Barrier 对齐（Barrier Alignment）情况下，如果设置该值大于0，则代表一开始会进行Barrier对齐操作，但是对齐等待超过此值，则会切换为非Barrier对齐（Barrier Alignment）
//        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(10));


        // 数据处理
        env
                .socketTextStream("127.0.0.1", 9999)
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1)
                 .print();
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
