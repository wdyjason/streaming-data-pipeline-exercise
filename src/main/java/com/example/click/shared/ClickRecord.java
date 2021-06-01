package com.example.click.shared;

import java.sql.Timestamp;

public class ClickRecord {
    private String itemId;
    private long count;
    private Timestamp timestamp;

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

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public ClickRecord(String itemId, long count, Timestamp timestamp) {
        this.itemId = itemId;
        this.count = count;
        this.timestamp = timestamp;
    }

    public ClickRecord() {
    }

    @Override
    public String toString() {
        return "Click{" +
                "itemId='" + itemId + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }
}
