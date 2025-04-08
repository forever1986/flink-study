package com.demo.lesson06.custom2;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class MinIOWriterState {

    private final int subtaskId;
    private final Tuple2<String, String> data;

    public MinIOWriterState(int subtaskId, Tuple2<String, String> data) {
        this.subtaskId = subtaskId;
        this.data = data;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public Tuple2<String, String> getData() {
        return data;
    }
}
