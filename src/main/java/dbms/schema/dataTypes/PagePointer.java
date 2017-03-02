package dbms.schema.dataTypes;

import dbms.storage.Page;

import java.nio.MappedByteBuffer;

/**
 * Created by alex on 02.03.17.
 */
public class PagePointer implements Cell {
    private Long index;
    private Short offset;

    public PagePointer(Long index, Short offset) {
        this.index = index;
        this.offset = offset;
    }

    public Long getIndex() {
        return index;
    }

    public void setIndex(Long index) {
        this.index = index;
    }

    public Short getOffset() {
        return offset;
    }

    public void setOffset(Short offset) {
        this.offset = offset;
    }

    public static PagePointer readPagePointer(MappedByteBuffer buffer) {
        return new PagePointer(buffer.getLong(), buffer.getShort());
    }

    @Override
    public short getByteSize() {
        return (short)(Long.BYTES + Short.BYTES);
    }

    @Override
    public void writeCell(MappedByteBuffer buffer) {
        buffer.putLong(index);
        buffer.putShort(offset);
    }

    @Override
    public int compareTo(Cell other) {
        return 0;
    }

    @Override
    public int compareTo(String other) {
        return 0;
    }
}
