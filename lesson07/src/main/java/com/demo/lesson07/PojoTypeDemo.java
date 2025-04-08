package com.demo.lesson07;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PojoTypeDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算
        text
                .map((String value) -> {
                    String[] values = value.split(",");
                    String value1 = values[0];
                    double value2 = Double.parseDouble("0");
                    long value3 = 0;
                    if (values.length >= 2) {
                        try {
                            value2 = Double.parseDouble(values[1]);
                        } catch (Exception e) {
                            value2 = Double.parseDouble("0");
                        }
                    }
                    if (values.length >= 3) {
                        try {
                            value3 = Long.parseLong(values[2]);
                        } catch (Exception ignored) {
                        }
                    }
                    return new ServerInfo(value1, new CPUInfo(value2, "%"), value3);
                })
                .print();
        // 执行
        env.execute();
    }

    // 1.必须是public
    public static class ServerInfo{

        // 2.属性要么是public，要么提供setter/getter方法
        public String serverId;
        // 3. 如果属性也是自定义的类型，那么这个类型也要符合1,2,3,4规则
        public CPUInfo cpuInfo;

        public Long time;

        // 4.必须提供一个无参构造函数
        public ServerInfo() {
        }

        public ServerInfo(String serverId, CPUInfo cpuInfo, Long time) {
            this.serverId = serverId;
            this.cpuInfo = cpuInfo;
            this.time = time;
        }

        public String getServerId() {
            return serverId;
        }

        public void setServerId(String serverId) {
            this.serverId = serverId;
        }

        public CPUInfo getCpuInfo() {
            return cpuInfo;
        }

        public void setCpuInfo(CPUInfo cpuInfo) {
            this.cpuInfo = cpuInfo;
        }

        public Long getTime() {
            return time;
        }

        public void setTime(Long time) {
            this.time = time;
        }
    }

    public static class CPUInfo{
        private Double cpu;
        private String unit;

        public CPUInfo() {
        }

        public CPUInfo(Double cpu, String unit) {
            this.cpu = cpu;
            this.unit = unit;
        }

        public Double getCpu() {
            return cpu;
        }

        public void setCpu(Double cpu) {
            this.cpu = cpu;
        }

        public String getUnit() {
            return unit;
        }

        public void setUnit(String unit) {
            this.unit = unit;
        }
    }
}
