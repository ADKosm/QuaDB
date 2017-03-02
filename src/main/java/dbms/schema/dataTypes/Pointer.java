package dbms.schema.dataTypes;

import java.nio.MappedByteBuffer;

/**
 * Created by alex on 02.03.17.
 */
public class Pointer implements Cell {
    private Long pointer;

    public Pointer(Long pointer) {
        this.pointer = pointer;
    }

    public static Pointer readPointer(MappedByteBuffer buffer) {
        return new Pointer(buffer.getLong());
    }

    @Override
    public void writeCell(MappedByteBuffer buffer) {
        buffer.putLong(pointer);
    }

    @Override
    public short getByteSize() {
        return Long.BYTES;
    }

    @Override
    public int compareTo(Cell other) {
        return 0;
    }

    @Override
    public int compareTo(String other) {
        return 0;
    }

    public Long getPointer() {
        return pointer;
    }
}
