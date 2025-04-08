package com.demo.lesson06;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class FileSinkDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 需要设置checkpoint，输出的文件才会进入finished，不然只会存在 in-progress 或者 pending 状态，也就是没法变成.log结尾的文件
        env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(2);
        // 2 读取数据- 来自自定义的GeneratorFunction，我们这里设置每秒生成100个字符串
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, String>) value -> "value" + value,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100), // 每秒钟生成100条数据
                Types.STRING
        );
        DataStreamSource<String> source = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource");
        // 3. 输出：
        FileSink<String> fileSink = FileSink
                // 按行输出：第一个参数是输出路径，第二个参数是数据序列化，这里由于是字符串，所以使用Flink提供的SimpleStringEncoder
                .<String>forRowFormat(new Path("E:/logs"), new SimpleStringEncoder<>())
                // 这是批量输出，这里就不演示
//                .forBulkFormat()
                // 配置文件分目录，如果不设置，则按照每小时分一个目录，可以通过forRowFormat方法进去看看使用的是DateTimeBucketAssigner默认构建方法
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH-mm", ZoneId.systemDefault()))
                // 配置生成的文件的名称，可以配置前缀和后缀
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("flink-").withPartSuffix(".log").build())
                // 配置文件滚动策略，这里设置10秒或者超过5KB就更新文件
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(5 * 1024))
                                .build()
                )
                .build();
        source.sinkTo(fileSink);
        // 执行
        env.execute();
    }
}
