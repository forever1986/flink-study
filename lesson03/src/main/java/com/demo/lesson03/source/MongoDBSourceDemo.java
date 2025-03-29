package com.demo.lesson03.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;

/**
 * 来自mongoDB的数据
 */
public class MongoDBSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据-来自mongoDB的数据，注意这里使用的是Builder编码风格
        MongoSource<String> mongoSource = MongoSource.<String>builder()
                .setUri("mongodb://localhost:27017/") // 必须设置；mongoDB的连接URL
                .setDatabase("industrymodel") // 必须设置；数据库名称
                .setCollection("tank_farm") // 必须设置；集合名称
                // 必须设置；设置 MongoDeserializationSchema 用于解析 MongoDB BSON 类型的文档
                .setDeserializationSchema(new MongoDeserializationSchema<String>() {
                    @Override
                    public String deserialize(BsonDocument document) {
                        return document.toJson();
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .setProjectedFields("_id", "tankFarmName", "tankFarmCode") // 返回的字段
                .setFetchSize(2048) // 设置每次循环读取时应该从游标中获取的行数
                .setLimit(10000) // 限制每个 reader 最多读取文档的数量
                .setNoCursorTimeout(false) // 连接关闭超时设置。默认为true（10分钟超时），设置为false（30分钟超时）
                /**
                 * 设置分区策略。 可选的分区策略有 SINGLE，SAMPLE，SPLIT_VECTOR，SHARDED 和 DEFAULT
                 * SINGLE：将整个集合作为一个分区。
                 * SAMPLE：通过随机采样的方式来生成分区，快速但可能不均匀。
                 * SPLIT_VECTOR：通过 MongoDB 计算分片的 splitVector 命令来生成分区，快速且均匀。 仅适用于未分片集合，需要 splitVector 权限。
                 * SHARDED：从 config.chunks 集合中直接读取分片集合的分片边界作为分区，不需要额外计算，快速且均匀。 仅适用于已经分片的集合，需要 config 数据库的读取权限。
                 * DEFAULT：对分片集合使用 SHARDED 策略，对未分片集合使用 SPLIT_VECTOR 策略。
                 */
                .setPartitionStrategy(PartitionStrategy.SINGLE)
                // 设置每个分区的内存大小。通过指定的分区大小，将 MongoDB 的一个集合切分成多个分区。 可以设置并行度，并行地读取这些分区，以提升整体的读取速度。
                .setPartitionSize(MemorySize.ofMebiBytes(64))
                // 仅用于 SAMPLE 抽样分区策略，设置每个分区的样本数量。抽样分区器根据分区键对集合进行随机采样的方式计算分区边界。 总的样本数量 = 每个分区的样本数量 * (文档总数 / 每个分区的文档数量)
                .setSamplesPerPartition(10)
                .build();
        // 注意，这里有一个WatermarkStrategy，是一个水位线，这个我们讲到水位线时再讲
        DataStreamSource<String> source = env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "mongoSource");
        // 3. 输出
        source.print();
        // 执行
        env.execute();
    }
}
