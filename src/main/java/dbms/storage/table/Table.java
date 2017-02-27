package dbms.storage.table;

import dbms.Consts;
import dbms.index.Index;
import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.Schema;
import dbms.schema.SchemaManager;
import javafx.scene.control.Tab;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by alex on 27.02.17.
 */

/**
 * Table store meta information of table
 * TableImplementation get created table and serve its
 */
public class Table {
    private SchemaManager schemaManager = SchemaManager.getInstance();

    private HashMap<Column, Index> indexes = new HashMap<>();
    private Schema schema;
    private TableImplementation implementation;
    private String name;
    private boolean isVirtual = true;
    private boolean isTemproary = false;

    private Integer usedVirtualMemory = 0;
    private Integer maxVirtualMemory = Consts.MAX_MEMORY_USED;

    public Table(Schema schema) { // anonimus table without name. Begin with virtual sroting
        this.schema = new Schema(schema.getColumns());
        this.name = new BigInteger(128, new SecureRandom()).toString(32);
        this.schema.setTableName(this.name);
        this.schema.setDataFilePath(schemaManager.getTempRoot() + "/" + this.name);
        this.implementation = new VirtualTable(this.schema, this.name);
        this.isTemproary = true;
    }

    public Table(Schema schema, String name) { // names table. Load real data
        isVirtual = false;
        this.schema = schema;
        this.name = name;
        implementation = new RealTable(schema, name);
    }

    public String getName() {
        return name;
    }

    public void clear() {
        if(isTemproary) {
            implementation.clear();
        }
    }

    private void storeTable() {
        isVirtual = false;

        TableImplementation realImplementation = RealTable.createNewRealTable(name, schema);
        Iterator<Row> rowIterator = implementation.iterator();
        while(rowIterator.hasNext()) {
            realImplementation.add(rowIterator.next());
        }
        implementation = realImplementation;
    }

    public Iterator<Row> iterator() {
        return implementation.iterator();
    }

    public void add(Row row) {
        if(isVirtual) {
            usedVirtualMemory += row.getRowSize();
            if(usedVirtualMemory > maxVirtualMemory) storeTable();
        }

        implementation.add(row);
    }

    public void addAll(List<Row> rows) {
        if(isVirtual) {
            for(Row row : rows) usedVirtualMemory += row.getRowSize();
            if(usedVirtualMemory > maxVirtualMemory) storeTable();
        }

        implementation.addAll(rows);
    }

    public Schema getSchema() {
        return schema;
    }
}
