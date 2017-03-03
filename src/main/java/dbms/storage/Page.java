package dbms.storage;

import dbms.Consts;
import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.Schema;
import dbms.schema.TableSchema;
import dbms.schema.dataTypes.Cell;
import dbms.schema.dataTypes.Int;
import dbms.schema.dataTypes.PagePointer;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**Head:
 * | Free space | Record count | Shift table | 0 | ... | data |
 * Record : | liveFlag | data |
 * liveFlag == 0 => record is dead
 * liveFlag == 1 => record is alive
 *
 * Shift table consists of short (2 bytes) values, which is a position of records in page
 *
 * After inserting\removing:
 * * Update liveFlag
 * * Update record in shift table
 * * Update Free Space
 * * Update Record Count
 *
 * By now deleted record are still take up space in page
 * In future, there should be an defragmentation operation, which must reconstruct page with a lot of dead records
 * But we have to call this operation quite rare, cause it will modify data on hard disk, indexes, and thats why is
 * is very expensive
 */

public class Page {
    private Integer freeSpace;
    private Integer recordCount;
    private Integer shiftTable = 2 * 4; // 2 * sizeof(int) = position of shift table

    private MappedByteBuffer buffer;

    public Page(MappedByteBuffer b, boolean clear) {
        buffer = b;
        if(clear) {
            recordCount = 0;
            freeSpace = Consts.BLOCK_SIZE - 4 - 4 - 1;
            //  Free space = Page size - |Free space| - |Record count| - |zero|
            buffer.putInt(freeSpace);
            buffer.putInt(recordCount);

            while (buffer.remaining() > 0) buffer.put((byte) 0); // clear page

            buffer.position(0);
        } else {
            freeSpace = buffer.getInt();
            recordCount = buffer.getInt();
        }
    }

    public Integer getRecordCount() {
        return recordCount;
    }


    public Row getRow(Short offset, Schema schema) {
        try {
            buffer.position(offset);
            boolean deleted = (buffer.get() == 0);
            if(deleted) return null;

            List<Cell> cells = new ArrayList<Cell>();
            for(Column column : schema.getColumns()) {
                cells.add(column.readCell(buffer));
            }
            return new Row(cells);
        } catch (Exception e ){ e.fillInStackTrace(); return null; }
    }

    public List<Row> toRows(Schema schema) {
        try{
            List<Row> rows = new ArrayList<Row>();
            for(int i = shiftTable; i < shiftTable + recordCount * 2; i += 2) {
                short pos = buffer.getShort(i);
                buffer.position(pos);
                boolean deleted = (buffer.get() == 0);
                if(deleted) continue;

                List<Cell> cells = new ArrayList<Cell>();
                for(Column column : schema.getColumns()) {
                    cells.add(column.readCell(buffer));
                }
                rows.add(new Row(cells));
            }
            buffer.position(shiftTable);
            return rows;
        } catch (Exception e) {
            e.fillInStackTrace();
            return null;
        }
    }

    public boolean canPlaced(Row row) {
        return freeSpace >= row.getRowSize() + 2; // |row| + |record in shift table|
    }

    public Short insertValues(Row row) {
        short newPosition;
        if(recordCount == 0) {
            newPosition = (short)(Consts.BLOCK_SIZE - row.getRowSize());
        } else {
            buffer.position(shiftTable + recordCount * 2 - 2); // last record in shift table
            newPosition = (short)(buffer.getShort() - row.getRowSize());
        }
        // write new position in shift table
        buffer.position(shiftTable + recordCount * 2);
        buffer.putShort(newPosition);
        // write data in new position
        buffer.position(newPosition);
        buffer.put((byte) 1); //alive
        for(Cell cell : row.getCells()) {
            cell.writeCell(buffer);
        }
        // update record count and free space
        recordCount++;
        freeSpace -= row.getRowSize() + 2; // |row| + |record in shift table|
        buffer.position(0);
        buffer.putInt(freeSpace);
        buffer.putInt(recordCount);
        buffer.force();
        return newPosition;
    }

    public void clear() {
        buffer.position(0);
        recordCount = 0;
        freeSpace = Consts.BLOCK_SIZE - 4 - 4 - 1;
        //  Free space = Page size - |Free space| - |Record count| - |zero|
        buffer.putInt(freeSpace);
        buffer.putInt(recordCount);

        while(buffer.remaining() > 0) buffer.put((byte) 0); // clear page

        buffer.position(0);
    }
}
