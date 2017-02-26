package dbms.schema.dataTypes;

import java.nio.MappedByteBuffer;
import java.sql.Date;
import java.time.Instant;

public class Datetime implements Cell {
    private Long value;

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public Datetime(Long v) {
        value = v;
    }

    public static Datetime readDatetime(MappedByteBuffer buffer) {
        return new Datetime(buffer.getLong());
    }

    @Override
    public String toString() {
        /*
        from Instant.now().getEpochSecond();
         */
        return Date.from(Instant.ofEpochSecond(value)).toString();
    }

    @Override
    public void writeCell(MappedByteBuffer buffer) {
        buffer.putLong(value);
    }

    @Override
    public short getByteSize() {
        return 8; // sizeof(long)
    }
}
