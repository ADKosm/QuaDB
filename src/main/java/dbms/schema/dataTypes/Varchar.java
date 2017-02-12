package dbms.schema.dataTypes;

import java.nio.MappedByteBuffer;

public class Varchar implements Cell {
    private String value;
    private byte size;

    public Varchar(String v, byte s) {
        value = v;
        size = s;
    }

    public String toString() {
        return value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public byte getSize() {
        return size;
    }

    public void setSize(byte size) {
        this.size = size;
    }

    @Override
    public void writeCell(MappedByteBuffer buffer) {
        byte[] bytes = value.getBytes();

        buffer.put(Byte.parseByte(Integer.toString(value.length()))); // TODO: change to more clever method
        buffer.put(bytes);
        for(int i = value.length(); i < size; i++) buffer.put((byte)0);
    }
}
