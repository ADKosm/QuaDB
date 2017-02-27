package dbms.schema.dataTypes;

import java.nio.MappedByteBuffer;

public class Varchar implements Cell {
    private String value;
    private byte size; // max size

    public Varchar(String v, byte s) {
        value = v;
        size = s;
    }

    public static Varchar readVarchar(MappedByteBuffer buffer, byte s) {
        byte len = buffer.get();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        String value = new String(bytes);
        return new Varchar(value, s);
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

        buffer.put(Byte.parseByte(Integer.toString(bytes.length))); // TODO: change to more clever method
        buffer.put(bytes);
    }

    @Override
    public short getByteSize() {
        return (short)(value.length() + 1);
    }

    @Override
    public int compareTo(Cell other) {
        if(other instanceof Varchar) {
            return value.compareTo(((Varchar) other).getValue());
        } else {
            return 0; // TODO: show error
        }
    }

    @Override
    public int compareTo(String other) {
        return value.compareTo(other);
    }
}
