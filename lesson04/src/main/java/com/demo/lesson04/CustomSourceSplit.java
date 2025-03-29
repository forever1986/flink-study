package com.demo.lesson04;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * 数据分片（SourceSplit）
 */
public class CustomSourceSplit implements SourceSplit {

    // 由于我们是按照文件分发的规则，因此数据分片（SourceSplit）我们只需要将文件的路径给源阅读器（SourceReader）即可
    private String path;

    public CustomSourceSplit() {
    }

    public CustomSourceSplit(String path) {
        this.path = path;
    }

    @Override
    public String splitId() {
        return path;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
