package com.demo.lesson09.util;

import org.apache.flink.api.common.functions.MapFunction;

public class ServerInfoMapFunction implements MapFunction<String, ServerInfo> {

    @Override
    public ServerInfo map(String value) throws Exception {
        String[] values = value.split(",");
        String value1 = values[0];
        double value2 = Double.parseDouble("0");
        long value3 = 0;
        if(values.length >= 2){
            try {
                value2 = Double.parseDouble(values[1]);
            }catch (Exception e){
                value2 = Double.parseDouble("0");
            }
        }
        if(values.length >= 3){
            try {
                value3 = Long.parseLong(values[2]);
            }catch (Exception ignored){
            }
        }
        return new ServerInfo(value1,value2,value3);
    }
}
