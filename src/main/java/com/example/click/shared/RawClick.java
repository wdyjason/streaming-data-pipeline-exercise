package com.example.click.shared;

import java.util.Objects;

public class RawClick {
    private String itemId;
    private long count;

    public RawClick() {
    }

    public RawClick(String itemId, long count) {
        this.itemId = itemId;
        this.count = count;
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

    @Override
    public String toString() {
        return "Click{" +
                "itemId='" + itemId + '\'' +
                ", count=" + count +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RawClick rawClick = (RawClick) o;
        return count == rawClick.count && Objects.equals(itemId, rawClick.itemId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, count);
    }
}
