package com.example.click;

import java.util.Objects;

public class Click {
    private String itemId;
    private long count;

    public Click() {
    }

    public Click(String itemId, long count) {
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
        Click click = (Click) o;
        return count == click.count && Objects.equals(itemId, click.itemId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, count);
    }
}
