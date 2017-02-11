package dbms.schema;

import dbms.Consts;

import java.util.HashMap;

public class Column {
    private String name;
    private int type;
    private int size = 0;

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

    public String toString() {
        HashMap<Integer, String> typesMap = new HashMap<Integer, String>();
        typesMap.put(Consts.COLUMN_TYPE_INTEGER, "int");
        typesMap.put(Consts.COLUMN_TYPE_VARCHAR, "varchar");
        typesMap.put(Consts.COLUMN_TYPE_DATETIME, "datetime");

        String result = size == 0 ?
                " " + name + " [ " + typesMap.get(type) + " ] " :
                " " + name + " [ " + typesMap.get(type) + " ( " + Integer.toString(size) + " ) ] ";

        return result;
    }
}
