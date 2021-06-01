package com.example.click;

public class Click {
    private String itemId;
    private long count;
    private long timestamp;


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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Click(String itemId, long count, long timestamp) {
        this.itemId = itemId;
        this.count = count;
        this.timestamp = timestamp;
    }

    public Click() {
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
