package com.demo.lesson08.model;

public class ServerAvgInfo {

    private String serverId;
    private Double cpuTotal;
    private Long num;

    public ServerAvgInfo() {
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public Double getCpuTotal() {
        return cpuTotal;
    }

    public void setCpuTotal(Double cpuTotal) {
        this.cpuTotal = cpuTotal;
    }

    public Long getNum() {
        return num;
    }

    public void setNum(Long num) {
        this.num = num;
    }
}
