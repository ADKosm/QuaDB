package dbms.schema.dataTypes;


import java.nio.MappedByteBuffer;

public class Int implements Cell {
    private Integer value;

    public Int(Integer v) {
        value = v;
    }

    public static Int readInt(MappedByteBuffer buffer) {
        return new Int(buffer.getInt());
    }

    public String toString() {
        return value.toString();
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public void writeCell(MappedByteBuffer buffer) {
        buffer.putInt(value);
    }

    @Override
    public short getByteSize() {
        return 4; // sizeof(int)
    }

    @Override
    public int compareTo(Cell other) {
        if(other instanceof Int) {
            return value - ((Int)other).getValue();
        } else {
            return 0;
        }
    }

    @Override
    public int compareTo(String other) {
        try {
            Integer otherInt = Integer.parseInt(other);
            return value - otherInt;
        } catch (Exception e) {
            return 0;
        }
    }
}
