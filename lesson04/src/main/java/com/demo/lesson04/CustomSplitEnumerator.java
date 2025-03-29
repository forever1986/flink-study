package com.demo.lesson04;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 分片枚举器（SplitEnumerator）
 */
public class CustomSplitEnumerator implements SplitEnumerator<CustomSourceSplit, CustomSplitEnumeratorCheckpoint> {


    // 分片枚举器（SplitEnumerator）上下文，用于与源阅读器（SourceReader）通讯
    private SplitEnumeratorContext<CustomSourceSplit> splitSplitEnumeratorContext;

    // 定时查询文件夹的时间间隔
    private long refreshInterval;

    // 文件路径
    private File fileDir;

    // 文件过滤器

    private LogFileFilter logFileFilter;

    /**
     * 用于保存待分配的数据分片（SourceSplit）
     */
    private LinkedList<CustomSourceSplit> sourceSplitList;

    /**
     * 用于保存空闲的CustomSourceReader
     */
    private List<Integer> sourceReaderList;

    /**
     * 已加载文件名称列表
     */
    private Set<String> loadedFiles;


    /**
     * 新建CustomSplitEnumerator
     */
    public CustomSplitEnumerator(SplitEnumeratorContext<CustomSourceSplit> splitSplitEnumeratorContext, long refreshInterval, String path) {
        this.splitSplitEnumeratorContext = splitSplitEnumeratorContext;
        this.refreshInterval = refreshInterval;
        this.fileDir = new File(path);
        this.logFileFilter = new LogFileFilter();
        this.sourceSplitList = new LinkedList<>();
        this.sourceReaderList = new ArrayList<>();
        this.loadedFiles = new ConcurrentSkipListSet<>();
    }

    /**
     * 从某个检查点新建CustomSplitEnumerator
     */
    public CustomSplitEnumerator(SplitEnumeratorContext<CustomSourceSplit> splitSplitEnumeratorContext, long refreshInterval, String path, LinkedList<CustomSourceSplit> sourceSplitList, Set<String> loadedFiles) {
        this.splitSplitEnumeratorContext = splitSplitEnumeratorContext;
        this.refreshInterval = refreshInterval;
        this.fileDir = new File(path);
        this.logFileFilter = new LogFileFilter();
        this.sourceSplitList = sourceSplitList;
        this.sourceReaderList = new ArrayList<>();
        this.loadedFiles = loadedFiles;
    }

    /**
     * 启动方法：这里使用SplitEnumeratorContext的callAsync方法启动一个定时任务（我们也可以自己写）
     */
    @Override
    public void start() {
        this.splitSplitEnumeratorContext.callAsync(
                this::loadFiles, // 定时轮询方法
                this::distributeSplit,  // 如果读取到数据，则使用该方法进行分片数据
                refreshInterval, // 启动多少秒之后开始读取文件夹
                refreshInterval);  // 间隔多少秒读取一次文件夹
    }

    /**
     * 定时扫描指定路径，如果发现新文件，返回数据
     */
    private List<CustomSourceSplit> loadFiles() {
        File[] files = fileDir.listFiles(logFileFilter);
        if (files==null|| files.length == 0) {
            return new ArrayList<>();
        }
        //将加载出的文件放入loadedFiles并包装为分片(FileSourceSplit)返回
        List<CustomSourceSplit> result = new ArrayList<>();
        for(File file : files){
            // 加入到列表中，防止重复
            if(loadedFiles.add(file.getAbsolutePath())){
                result.add(new CustomSourceSplit(file.getAbsolutePath()));
            }
        }
        return result;
    }


    /**
     * 如果读取到数据，则使用该方法进行分片数据，其中assignSplit方法就是分配给源阅读器（SourceReader）
     */
    private void distributeSplit(List<CustomSourceSplit> list, Throwable error) {
        sourceSplitList.addAll(list);
        if (sourceSplitList.isEmpty()) {
            return;
        }
        // 看看空闲的sourceReaderList列表有没有空闲的源阅读器（SourceReader）
        Iterator<Integer> iterator = sourceReaderList.iterator();
        while (iterator.hasNext()) {
            Integer next = iterator.next();
            // 拿出一个数据分片
            CustomSourceSplit poll = sourceSplitList.poll();
            if (poll == null) {
                break;
            }
            // 设置给源阅读器（SourceReader）
            splitSplitEnumeratorContext.assignSplit(poll, next);
            iterator.remove();
        }
    }


    /**
     * 源阅读器（SourceReader）调用SourceReaderContext#sendSplitRequest()，
     * 就是告诉分片枚举器（SplitEnumerator）自己空闲，分片枚举器（SplitEnumerator）可以将其加入到空闲列表
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        sourceReaderList.add(subtaskId);
    }

    /**
     * 将数据分片（SourceSplit）添加回分片枚举器（SplitEnumerator）。
     * 只有在源阅读器（SourceReader）发生异常读取失败，且没有被检查点保存下来时，告诉SplitEnumerator分片枚举器，这些分片没有被处理
     */
    @Override
    public void addSplitsBack(List splits, int subtaskId) {
        sourceSplitList.addAll(splits);
    }

    /**
     * 添加源阅读器（SourceReader），这里在我们本次演示无需做任何内容
     */
    @Override
    public void addReader(int subtaskId) {

    }

    /**
     * 定时的快照方法，也就是保存当前处理的情况，防止程序挂了，可以从检查点开始，而不是从头开始
     */
    @Override
    public CustomSplitEnumeratorCheckpoint snapshotState(long checkpointId) throws Exception {
        return new CustomSplitEnumeratorCheckpoint(loadedFiles, sourceSplitList);
    }

    /**
     * 关闭方法，这里在我们本次演示无需做任何内容
     */
    @Override
    public void close() throws IOException {

    }

    /**
     * 处理SourceEvent。这个方法是用于监听SourceEvent事件，可用于与源阅读器（SourceReader）交互
     * 这里监听来自源阅读器（SourceReader）的CustomSourceEvent事件，删除loadedFiles中已经完成读取的文件，防止loadedFiles过大
     * 这个方法会将sourceReader已完成读取并已通过检查点的文件删除。
     */
    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof CustomSourceEvent) {
            Set<String> fileSplits = ((CustomSourceEvent) sourceEvent).getSplits();
            loadedFiles.removeAll(fileSplits);
        }
    }

    /**
     * 过滤文件名，只读取log结尾的文件名且是文件的
     */
    public static class LogFileFilter implements FileFilter {

        private String suffixName = ".log";

        public LogFileFilter() {
        }

        public LogFileFilter(String suffixName) {
            this.suffixName = suffixName;
        }

        @Override
        public boolean accept(File pathname) {
            return pathname.isFile() && pathname.getName().endsWith(suffixName);
        }
    }
}
