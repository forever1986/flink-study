package com.demo.lesson04;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink的执行类
 */
public class CustomSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(60000); // 配置每60秒自动保存一个检查点
        // 2. 读取数据 -- 使用我们自定义的数据源。读取E:\mylogs目录，并且设置每隔5秒读取一次目录里面的文件列表
        CustomSource customSource = new CustomSource("E:\\mylogs", 10000);
        DataStreamSource<String> dataStreamSource = env.fromSource(customSource, WatermarkStrategy.noWatermarks(), "custom");
        dataStreamSource.setParallelism(1); // 这里配置1个并行度，只是为了方便debug
        // 3. 输出
        dataStreamSource.print();
        // 执行
        env.execute();
    }

}
