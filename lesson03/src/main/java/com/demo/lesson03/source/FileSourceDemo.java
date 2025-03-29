package com.demo.lesson03.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 来自文件的数据
 */
public class FileSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据-来自文件
//        env.readTextFile("lesson01/input/sentence.txt"); // 该方法已经废弃，现在是通过fromSource方式
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), // 处理来自文件的数据方式，这里使用一个TextLine方式（按行处理）
                new Path("lesson03/input/sentence.txt") // 来自那个文件的路径
        ).build();
        // 注意，这里有一个WatermarkStrategy，是一个水位线，这个我们讲到水位线时再讲
        DataStreamSource<String> source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource");
        // 3. 输出
        source.print();
        // 执行
        env.execute();
    }

}
