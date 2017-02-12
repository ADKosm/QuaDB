package dbms.schema.dataTypes;


import java.nio.MappedByteBuffer;

public class Int implements Cell {
    private Integer value;

    public Int(Integer v) {
        value = v;
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
}
