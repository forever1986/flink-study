package com.demo.lesson04;

import com.alibaba.fastjson.JSON;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * 数据分配的序列化，由于分片枚举器（SplitEnumerator）和源阅读器（SourceReader）可能在不同服务器，因此数据分片需要序列化传输。这里使用alibaba的fastjson组件做序列化
 */
public class CustomSplitSerializer implements SimpleVersionedSerializer<CustomSourceSplit> {


    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(CustomSourceSplit obj) throws IOException {
        return JSON.toJSONBytes(obj);
    }

    @Override
    public CustomSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        return JSON.parseObject(serialized, CustomSourceSplit.class);
    }
}
