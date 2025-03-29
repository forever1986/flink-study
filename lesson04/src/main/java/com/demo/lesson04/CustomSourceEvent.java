package com.demo.lesson04;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Set;

/**
 * SourceEvent事件，用于源阅读器（SourceReader）删除文件后通知分片枚举器（SplitEnumerator）
 */
public class CustomSourceEvent implements SourceEvent {

    private Set<String> splits;

    public CustomSourceEvent() {
    }

    public CustomSourceEvent(Set<String> splits) {
        this.splits = splits;
    }

    public Set<String> getSplits() {
        return splits;
    }

    public void setSplits(Set<String> splits) {
        this.splits = splits;
    }
}
