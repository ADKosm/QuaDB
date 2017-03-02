package dbms.storage.table;

import dbms.schema.Row;
import dbms.schema.dataTypes.PagePointer;
import dbms.storage.Page;

import java.util.Iterator;
import java.util.List;

/**
 * Created by alex on 27.02.17.
 */
public interface TableImplementation {
    Iterator<Row> iterator();

    PagePointer add(Row row);
    void addAll(List<Row> rows);

    void clear();
}
