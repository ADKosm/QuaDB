package dbms.storage;

import dbms.Consts;
import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.Schema;
import dbms.schema.dataTypes.Cell;
import dbms.schema.dataTypes.Int;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**Head:
 * | recordCount | recordSize | data |
 * Record : | liveFlag | data |
 * liveFlag == 0 => record is dead
 * liveFlag == 1 => record is alive
 */

public class Page {
    private Integer recordSize;
    private Integer recordCount;
    private Integer headSize = 2 * 4; // 2 * sizeof(int)

    private MappedByteBuffer buffer;

    public Page(MappedByteBuffer b) {
        buffer = b;
        recordCount = buffer.getInt();
        recordSize = buffer.getInt();
    }

    public static Page createPage(MappedByteBuffer b, Schema schema) {
        Integer rCount = 0;
        Integer rSize = schema.getRowSize();
        b.putInt(rCount);
        b.putInt(rSize);

        while(b.remaining() > 0) b.put((byte) 0); // TODO: change to more clever method

        b.position(0);

        return new Page(b);
    }

    public Integer getRecordSize() {
        return recordSize;
    }

    public void setRecordSize(Integer recordSize) {
        this.recordSize = recordSize;
    }

    public Integer getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(Integer recordCount) {
        this.recordCount = recordCount;
    }

    public List<Row> toRows(Schema schema) {
        try{
            List<Row> rows = new ArrayList<Row>();
            while(buffer.remaining() >= recordSize) {
                boolean deleted = (buffer.get() == 0);

                List<Cell> cells = new ArrayList<Cell>();
                for(Column column : schema.getColumns()){
                    cells.add(column.readCell(buffer));
                }

                if(deleted) continue; // TODO: change useless reading to position changing
                rows.add(new Row(cells));
            }
            buffer.position(headSize);
            return rows;
        } catch (Exception e) {
            e.fillInStackTrace();
            return null;
        }
    }

    public boolean isFull() {
        Integer available = (Consts.BLOCK_SIZE - headSize) / recordSize;
        return recordCount >= available;
    }

    public void insertValues(Row row) {
        for(int i = headSize; buffer.remaining() >= recordSize; i += recordSize) {
            buffer.position(i);
            boolean deleted = (buffer.get() == 0);
            if(deleted) {
                buffer.position(i);
                buffer.put((byte) 1); // alive
                for(Cell cell : row.getCells()) {
                    cell.writeCell(buffer);
                }
                recordCount++;

                buffer.position(0);
                buffer.putInt(recordCount);
                buffer.position(headSize);
                break;
            }
        }
    }
}
