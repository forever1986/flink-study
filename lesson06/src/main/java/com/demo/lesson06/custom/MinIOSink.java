package com.demo.lesson06.custom;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;

/**
 * Sink是用于定义所需的内容，比如SinkWriter等
 */
public class MinIOSink implements Sink<String> {

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;

    public MinIOSink(String endpoint, String accessKey, String secretKey, String bucket) {
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
    }

    /**
     * 定义写入的SinkWriter，会被taskManager调用(该方法已经废弃，可使用新的方法）
     */
    @Override
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        return new MinIoSinkWriter(endpoint, accessKey, secretKey, bucket, context.getTaskInfo().getIndexOfThisSubtask());
    }

    /**
     * 定义写入的SinkWriter，会被taskManager调用
     */
    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new MinIoSinkWriter(endpoint, accessKey, secretKey, bucket, context.getTaskInfo().getIndexOfThisSubtask());
    }
}







