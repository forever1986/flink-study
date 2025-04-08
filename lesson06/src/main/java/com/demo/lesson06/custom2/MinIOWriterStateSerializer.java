package com.demo.lesson06.custom2;

import com.alibaba.fastjson.JSON;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class MinIOWriterStateSerializer implements SimpleVersionedSerializer<MinIOWriterState> {
    @Override
    public int getVersion() { return 1; }

    @Override
    public byte[] serialize(MinIOWriterState state) throws IOException {
        System.out.println("MinIOWriterStateSerializer.serialize");
        return JSON.toJSONBytes(state);
    }

    @Override
    public MinIOWriterState deserialize(int version, byte[] serialized) throws IOException {
        System.out.println("MinIOWriterStateSerializer.deserialize");
        return JSON.parseObject(serialized, MinIOWriterState.class);
    }
}

