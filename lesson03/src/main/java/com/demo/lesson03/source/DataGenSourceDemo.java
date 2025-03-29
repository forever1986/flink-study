package com.demo.lesson03.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 来自生成的数据
 */
public class DataGenSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.1 读取数据-来自已经实现的FromElementsGeneratorFunction
//        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
//                new FromElementsGeneratorFunction<String>(Types.STRING, "1","22","35","56")
//                ,4
//                ,Types.STRING);
        // 2.2 读取数据- 来自自定义的GeneratorFunction
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>(){
                    @Override
                    public String map(Long value) throws Exception {
                        return "value"+value;
                    }
                },
                4,
                Types.STRING
        );
        // 注意，这里有一个WatermarkStrategy，是一个水位线，这个我们讲到水位线时再讲
        DataStreamSource<String> source = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource");
        // 3. 输出
        source.print();
        // 执行
        env.execute();
    }

}
