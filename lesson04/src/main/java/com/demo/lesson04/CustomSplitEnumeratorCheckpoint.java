package com.demo.lesson04;

import java.util.LinkedList;
import java.util.Set;

/**
 * 检查点，只需要保存已加载文件名称列表和用于保存待分配的数据分片（SourceSplit）
 */
public class CustomSplitEnumeratorCheckpoint {


    /**
     * 存储已加载的文件
     */
    private Set<String> loadedFiles;
    /**
     * 用于保存待分配的数据分片（SourceSplit）--因为还没有分配给源阅读器（SourceReader）处理
     */
    private LinkedList<CustomSourceSplit> sourceSplitList;

    public CustomSplitEnumeratorCheckpoint(Set<String> loadedFiles, LinkedList<CustomSourceSplit> sourceSplitList) {
        this.loadedFiles = loadedFiles;
        this.sourceSplitList = sourceSplitList;
    }

    public Set<String> getLoadedFiles() {
        return loadedFiles;
    }

    public void setLoadedFiles(Set<String> loadedFiles) {
        this.loadedFiles = loadedFiles;
    }

    public LinkedList<CustomSourceSplit> getSourceSplitList() {
        return sourceSplitList;
    }

    public void setSourceSplitList(LinkedList<CustomSourceSplit> sourceSplitList) {
        this.sourceSplitList = sourceSplitList;
    }
}
