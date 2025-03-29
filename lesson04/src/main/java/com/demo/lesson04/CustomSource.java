package com.demo.lesson04;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * 自定义的Source
 */
public class CustomSource implements Source<String, CustomSourceSplit, CustomSplitEnumeratorCheckpoint> {

    // 文件路径
    private final String dir;

    // 定时查询文件夹的时间间隔
    private final long refreshInterval;

    public CustomSource(String dir, long refreshInterval) {
        this.dir = dir;
        this.refreshInterval = refreshInterval;
    }

    /**
     * 设置该源算子是有界流还是无界流
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     *  创建分片枚举器（SplitEnumerator）
     */
    @Override
    public SplitEnumerator<CustomSourceSplit, CustomSplitEnumeratorCheckpoint> createEnumerator(SplitEnumeratorContext<CustomSourceSplit> enumContext) throws Exception {
        return new CustomSplitEnumerator(enumContext, refreshInterval, dir);
    }

    /**
     *  从某个检查点创建分片枚举器（SplitEnumerator），
     */
    @Override
    public SplitEnumerator<CustomSourceSplit, CustomSplitEnumeratorCheckpoint> restoreEnumerator(SplitEnumeratorContext<CustomSourceSplit> enumContext, CustomSplitEnumeratorCheckpoint checkpoint) throws Exception {
        return new CustomSplitEnumerator(enumContext, refreshInterval, dir, checkpoint.getSourceSplitList(), checkpoint.getLoadedFiles());
    }

    /**
     * 创建数据分片（SourceSplit）的序列化类
     */
    @Override
    public SimpleVersionedSerializer<CustomSourceSplit> getSplitSerializer() {
        return new CustomSplitSerializer();
    }

    /**
     * 创建检查点的序列化类
     */
    @Override
    public SimpleVersionedSerializer<CustomSplitEnumeratorCheckpoint> getEnumeratorCheckpointSerializer() {
        return new CustomEnumeratorCheckpointSerializer();
    }

    /**
     * 创建源阅读器（SourceReader）
     */
    @Override
    public SourceReader<String, CustomSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new CustomSourceReader(readerContext);
    }
}
