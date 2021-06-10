package streamingdatapipelineexercise.examples.click.shared;

public class Item {
    private String itemId;

    public Item() {
    }

    public Item(String itemId, String description) {
        this.itemId = itemId;
        this.description = description;
    }

    @Override
    public String toString() {
        return "Item{" +
                "itemId='" + itemId + '\'' +
                ", description='" + description + '\'' +
                '}';
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    private String description;
}
