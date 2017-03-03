package dbms.schema;

import dbms.Consts;
import dbms.schema.dataTypes.*;

import java.nio.MappedByteBuffer;
import java.time.Instant;
import java.util.HashMap;

public class Column {
    private String name;
    private int type;
    private byte size = 0;

    public Column(String name, int type){
        this.name = name;
        this.type = type;
    }

    public Column(String name, int type, byte size){
        this.name = name;
        this.type = type;
        this.size = size;
    }

    public String toString() {
        HashMap<Integer, String> typesMap = new HashMap<Integer, String>();
        typesMap.put(Consts.COLUMN_TYPE_INTEGER, "int");
        typesMap.put(Consts.COLUMN_TYPE_VARCHAR, "varchar");
        typesMap.put(Consts.COLUMN_TYPE_DATETIME, "datetime");
        typesMap.put(Consts.COLUMN_TYPE_POINTER, "pointer");
        typesMap.put(Consts.COLUMN_TYPE_PAGEPOINTER, "pagepointer");

        String result = size == 0 ?
                " " + name + " [ " + typesMap.get(type) + " ] " :
                " " + name + " [ " + typesMap.get(type) + " ( " + Byte.toString(size) + " ) ] ";

        return result;
    }

    public String getName() {
        return name;
    }

    public Cell readCell(MappedByteBuffer buffer) throws Exception {
        switch (type) {
            case Consts.COLUMN_TYPE_INTEGER:
                return Int.readInt(buffer);
            case Consts.COLUMN_TYPE_DATETIME:
                return Datetime.readDatetime(buffer);
            case Consts.COLUMN_TYPE_VARCHAR:
                return Varchar.readVarchar(buffer, size);
            case Consts.COLUMN_TYPE_POINTER:
                return Pointer.readPointer(buffer);
            case Consts.COLUMN_TYPE_PAGEPOINTER:
                return PagePointer.readPagePointer(buffer);
        }
        throw new Exception("Unknown column type");
    }

    public Integer getMaxSize() {
        switch (type) {
            case Consts.COLUMN_TYPE_INTEGER:
                return Integer.BYTES;
            case Consts.COLUMN_TYPE_DATETIME:
                return Long.BYTES;
            case Consts.COLUMN_TYPE_VARCHAR:
                return Integer.valueOf((int) size) + 1; // saving and size
            case Consts.COLUMN_TYPE_PAGEPOINTER:
                return Long.BYTES + Short.BYTES;
            case Consts.COLUMN_TYPE_POINTER:
                return Long.BYTES;
        }
        return 0;
    }

    public Cell createCell(String value) throws Exception {
        switch (type) {
            case Consts.COLUMN_TYPE_INTEGER:
                return new Int(Integer.parseInt(value));
            case Consts.COLUMN_TYPE_VARCHAR:
                String v = value.length() > size ? value.substring(0, size) : value;
                return new Varchar(v, size);
            case Consts.COLUMN_TYPE_DATETIME:
                if(value.equals("NOW"))  {
                    return new Datetime(Instant.now().getEpochSecond());
                } else {
                    return new Datetime(Instant.now().getEpochSecond()); // TODO: change to normal parsing
                }
        }
        throw new Exception("Impossible situation");
    }
}
