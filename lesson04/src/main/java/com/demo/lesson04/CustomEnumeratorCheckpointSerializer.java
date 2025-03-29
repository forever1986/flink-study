package com.demo.lesson04;

import com.alibaba.fastjson.JSON;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * 检查点的序列化，由于检查点需要保存到本地磁盘，因此需要序列化。这里使用alibaba的fastjson组件做序列化
 */
public class CustomEnumeratorCheckpointSerializer implements SimpleVersionedSerializer<CustomSplitEnumeratorCheckpoint> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(CustomSplitEnumeratorCheckpoint obj) throws IOException {
        return JSON.toJSONBytes(obj);
    }

    @Override
    public CustomSplitEnumeratorCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        return JSON.parseObject(serialized, CustomSplitEnumeratorCheckpoint.class);
    }
}
