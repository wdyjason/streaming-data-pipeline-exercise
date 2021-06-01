package com.example.click.shared;

import java.sql.Timestamp;

public class WindowClickRecord {
    private String itemId;
    private long count;

    public WindowClickRecord(String itemId, long count, Timestamp startTime, Timestamp endTime) {
        this.itemId = itemId;
        this.count = count;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public WindowClickRecord() {
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public void setEndTime(Timestamp endTime) {
        this.endTime = endTime;
    }

    private Timestamp startTime;
    private Timestamp endTime;

    @Override
    public String toString() {
        return "WindowClickRecord{" +
                "itemId='" + itemId + '\'' +
                ", count=" + count +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
