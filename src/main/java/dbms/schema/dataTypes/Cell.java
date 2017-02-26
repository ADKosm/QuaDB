package dbms.schema.dataTypes;

import java.nio.MappedByteBuffer;

public interface Cell {
    String toString();

    void writeCell(MappedByteBuffer buffer);

    short getByteSize();
}
