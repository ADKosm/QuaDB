package dbms.storage.table;

import dbms.schema.Row;
import dbms.schema.TableSchema;
import dbms.schema.dataTypes.PagePointer;
import dbms.storage.BufferManager;
import dbms.storage.Page;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * Created by alex on 27.02.17.
 */
public class RealTable implements TableImplementation {
    private TableSchema schema;
    private String name;

    private BufferManager bufferManager;

    public RealTable(TableSchema schema, String name) {
        this.schema = schema;
        this.name = name;
        bufferManager = new BufferManager(this.schema);
    }

    public static RealTable createNewRealTable (String name, TableSchema schema) { // TODO: adaptate to real creating table by query
        try{
            new File(schema.getDataFilePath()).createNewFile();
            return new RealTable(schema, name);
        } catch (Exception e) {
            e.fillInStackTrace();
            return null;
        }
    }

    @Override
    public PagePointer add(Row row) {
        Page page = bufferManager.getLastPage();

        if(page == null || !page.canPlaced(row)) {
            page = bufferManager.allocateNewPage();
        }

        return new PagePointer(bufferManager.getPageCount()-1,  page.insertValues(row));
    }

    @Override
    public void addAll(List<Row> rows) {
        for(Row row : rows) { // TODO: optimize
            add(row);
        }
    }

    public Row getRowAt(PagePointer pointer) {
        return bufferManager.getPage(pointer.getIndex()).getRow(pointer.getOffset(), schema);
    }

    public TableSchema getSchema() {
        return schema;
    }

    public class RealTableIterator implements Iterator<Row> {
        private Long offset;
        private int index;

        private List<Row> currentRows;
        private Page currentPage;

        public RealTableIterator() {
            offset = (long)0;
            index = 0;
            currentPage = bufferManager.getPage(offset);
            if(currentPage != null) {
                currentRows = currentPage.toRows(schema);
            }
        }

        @Override
        public boolean hasNext() {
            return currentPage != null;
        }

        @Override
        public Row next() {
            Row result = currentRows.get(index);
            index++;
            if(index >= currentRows.size()) {
                index = 0;
                offset++;
                currentPage = bufferManager.getPage(offset);
                if(currentPage != null) {
                    currentRows = currentPage.toRows(schema);
                }
            }
            return result;
        }
    }

    @Override
    public void remove(Row row) {
        PagePointer pointer  = row.getPagePointer();
        bufferManager.getPage(pointer.getIndex()).removeRow(pointer.getOffset(), schema);
    }

    @Override
    public Iterator<Row> iterator() {
        return new RealTableIterator();
    }

    @Override
    public void clear() {
        new File(schema.getDataFilePath()).delete();
    }
}
