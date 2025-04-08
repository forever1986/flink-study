package com.demo.lesson09.util;

import org.apache.flink.api.java.functions.KeySelector;

public class KeySelectorFunction implements KeySelector<ServerInfo, String> {
    @Override
    public String getKey(ServerInfo value) throws Exception {
        // 返回第一个值，作为keyBy的分类
        return value.getServerId();
    }
}
