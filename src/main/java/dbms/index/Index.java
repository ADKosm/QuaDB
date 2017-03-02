package dbms.index;

import dbms.query.Predicate;
import dbms.schema.Row;
import dbms.schema.dataTypes.PagePointer;
import dbms.storage.table.RealTable;
import dbms.storage.table.Table;
import javafx.scene.control.Tab;

public class Index {
    private RealTable table;
    private IndexSchema schema;
    private BTree tree;

    public Index(IndexSchema indexSchema) {
        this.schema = indexSchema;
        this.tree = new BTree(indexSchema, table);
    }

    public void setTable(RealTable table) {
        this.table = table;
    }

    public Table search(Predicate predicate) {
        Table table = new Table(this.table.getSchema());
        tree.search(table, predicate);
        return table;
    }

    public void add(Row row, PagePointer pointer) {
        tree.insert(row.getCells().get(table.getSchema().getColumns().indexOf(schema.getIndexedColumn())), pointer);
    }
}
