package com.demo.lesson09.util;

/**
 * 记录服务器信息
 */
public class ServerInfo {

    private String serverId;
    private Double cpu;
    private Long time;

    public ServerInfo() {
    }

    public ServerInfo(String serverId, Double cpu, Long time) {
        this.serverId = serverId;
        this.cpu = cpu;
        this.time = time;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public Double getCpu() {
        return cpu;
    }

    public void setCpu(Double cpu) {
        this.cpu = cpu;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "serverId="+serverId+" cpu="+cpu+" time="+time;
    }
}
