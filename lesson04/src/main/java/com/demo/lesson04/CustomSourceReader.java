package com.demo.lesson04;

import cn.hutool.core.io.FileUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

/**
 * 源阅读器（SourceReader）
 */
public class CustomSourceReader implements SourceReader<String, CustomSourceSplit> {

    /**
     * 源阅读器（SourceReader）的上下文，用于跟分片枚举器（SplitEnumerator）通讯使用
     */
    private final SourceReaderContext sourceReaderContext;

    /**
     * 用于存放从文件中读取的数据的，由于是按行读取，因此每一行存储一个String
     */
    private Queue<String> waitQueue = new ConcurrentLinkedDeque<>();

    /**
     * 用于存放从分片枚举器（SplitEnumerator）接收到的分片
     */
    private List<CustomSourceSplit> allSplit = new ArrayList<>();

    /**
     * 用于存储已读取完成的分片,可用于删除
     */
    private List<CustomSourceSplit> finishSplit;

    /**
     * 用于标记数据是否读取完成
     */
    private CompletableFuture<Void> completableFuture;


    public CustomSourceReader(SourceReaderContext sourceReaderContext) {
        this.sourceReaderContext = sourceReaderContext;
        this.completableFuture = new CompletableFuture<>();
        this.finishSplit = new ArrayList<>();
    }

    /**
     * 源阅读器（SourceReader）的启动，这里在我们本次演示无需做任何内容
     */
    @Override
    public void start() {
    }

    /**
     * 源阅读器（SourceReader）数据处理函数，这里从waitQueue中读到数据，并发送到下游算子，不做任何处理
     */
    @Override
    public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
        if (!waitQueue.isEmpty()) {
            String poll = waitQueue.poll();
            if (StringUtils.isNotEmpty(poll)) {
                output.collect(poll);
            }
            /**
             * MORE_AVAILABLE - SourceReader 有可用的记录。
             * NOTHING_AVAILABLE - SourceReader 现在没有可用的记录，但是将来可能会有记录可用。
             * END_OF_INPUT - SourceReader 已经处理完所有记录，到达数据的尾部。这意味着 SourceReader 可以终止任务了。
             */
            return InputStatus.MORE_AVAILABLE;
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    /**
     * 保存快照方法
     */
    @Override
    public List<CustomSourceSplit> snapshotState(long checkpointId) {
        if (allSplit.isEmpty()) {
            return new ArrayList<>();
        }
        synchronized (allSplit) {
            finishSplit.addAll(allSplit);
            allSplit.clear();
        }
        return finishSplit;
    }

    /**
     * 是否已经处理完数据，如果处理完，调用SourceReaderContext#sendSplitRequest()通知分片枚举器（SplitEnumerator）自己空闲可以分配新数据
     */
    @Override
    public CompletableFuture<Void> isAvailable() {
        if (this.completableFuture.isDone()) {
            this.completableFuture = new CompletableFuture<>();
        }
        //分片枚举器（SplitEnumerator）
        this.sourceReaderContext.sendSplitRequest();
        return this.completableFuture;
    }

    /**
     * 拿到分片枚举器（SplitEnumerator）分给自己的数据分片（SourceSplit）
     * 按行读取文件并将读取每行数据放到该源阅读器（SourceReader）的waitQueue，供pollNext处理数据
     */
    @Override
    public void addSplits(List<CustomSourceSplit> splits) {
        for (CustomSourceSplit split : splits) {
            //按行读取数据
            List<String> lines = FileUtil.readLines(split.getPath(), StandardCharsets.UTF_8);
            waitQueue.addAll(lines);
        }
        //设置数据已经读取完成
        this.completableFuture.complete(null);
        //将分片加入到allSplit
        synchronized (allSplit) {
            allSplit.addAll(splits);
        }
    }

    /**
     * 当该源阅读器（SourceReader）不想再处理数据分片（SourceSplit）时，调用该方法。这里在我们本次演示无需做任何内容
     */
    @Override
    public void notifyNoMoreSplits() {

    }

    /**
     * 当该源阅读器（SourceReader）关闭时，调用该方法。这里在我们本次演示无需做任何内容
     */
    @Override
    public void close() throws Exception {

    }

    /**
     * 当所有检查点执行完成时调用该方法，这里我们清理掉已经读取的日志文件
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (finishSplit.isEmpty()) {
            return;
        }
        //将已完成检查点的数据删除
        Set<String> deleteFile = finishSplit.stream().map(it -> {
            FileUtil.del(it.getPath());
            return it.getPath();
        }).collect(Collectors.toSet());
        finishSplit.clear();
        //将已删除的文件通过事件通知分片枚举器（SplitEnumerator），分片枚举器（SplitEnumerator）可以删除loadedFile列表
        this.sourceReaderContext.sendSourceEventToCoordinator(new CustomSourceEvent(deleteFile));
    }
}
