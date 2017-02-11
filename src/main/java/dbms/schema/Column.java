package dbms.schema;

public class Column {
    private String name;
    private int type;
    private int size;

    public Column(String name, int type){
        this.name = name;
        this.type = type;
    }

    public Column(String name, int type, int size){
        this.name = name;
        this.type = type;
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
