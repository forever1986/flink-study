package com.demo.lesson06.custom2;

import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;

/**
 * 用于定义Sink所需的一系列内容
 */
public class MinIOSink implements Sink<Tuple2<String, String>>, SupportsWriterState<Tuple2<String, String>, MinIOWriterState> {

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
     * 定义写入的SinkWriter，会被taskManager调用
     */
    @Override
    public SinkWriter<Tuple2<String, String>> createWriter(InitContext context) throws IOException {
        return new MinIoSinkWriter(endpoint, accessKey, secretKey, bucket, context.getTaskInfo().getIndexOfThisSubtask());
    }


    @Override
    public StatefulSinkWriter<Tuple2<String, String>, MinIOWriterState> restoreWriter(WriterInitContext context, Collection<MinIOWriterState> recoveredState) throws IOException {
        System.out.println("MinIOSink.restoreWriter");
        return new MinIoSinkWriter(endpoint, accessKey, secretKey, bucket, context.getTaskInfo().getIndexOfThisSubtask());
    }

    @Override
    public SimpleVersionedSerializer<MinIOWriterState> getWriterStateSerializer() {
        System.out.println("MinIOSink.getWriterStateSerializer");
        return new MinIOWriterStateSerializer();
    }
}







