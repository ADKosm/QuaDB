package dbms.storage.table;

import dbms.schema.Row;

import java.util.Iterator;
import java.util.List;

/**
 * Created by alex on 27.02.17.
 */
public interface TableImplementation {
    Iterator<Row> iterator();

    void add(Row row);
    void addAll(List<Row> rows);

    void clear();
}
