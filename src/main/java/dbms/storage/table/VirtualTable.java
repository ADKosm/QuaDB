package dbms.storage.table;

import dbms.schema.Row;
import dbms.schema.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by alex on 27.02.17.
 */
public class VirtualTable implements TableImplementation {
    private Schema schema;
    private String name;
    private List<Row> rows = new ArrayList<>();

    public VirtualTable(Schema schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    @Override
    public Iterator<Row> iterator() {
        return rows.iterator();
    }

    @Override
    public void add(Row row) {
        rows.add(row);
    }

    @Override
    public void addAll(List<Row> rows) {
        rows.addAll(rows);
    }

    @Override
    public void clear() {
        // nothing
    }
}
