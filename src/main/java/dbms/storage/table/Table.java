package dbms.storage.table;

import dbms.Consts;
import dbms.index.Index;
import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.TableSchema;
import dbms.schema.SchemaManager;
import dbms.schema.dataTypes.PagePointer;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by alex on 27.02.17.
 */

/**
 * Table store meta information of table
 * TableImplementation get created table and serve its
 */
public class Table {
    private SchemaManager schemaManager = SchemaManager.getInstance();

    private HashMap<String, Index> indexes = new HashMap<>();
    private TableSchema schema;
    private TableImplementation implementation;
    private String name;
    private boolean isVirtual = true;
    private boolean isTemproary = false;

    private Integer usedVirtualMemory = 0;
    private Integer maxVirtualMemory = Consts.MAX_MEMORY_USED;

    private ReentrantLock mutex = new ReentrantLock();
    private Condition condition = mutex.newCondition();

    public Table(TableSchema schema) { // anonimus table without name. Begin with virtual sroting
        this.schema = new TableSchema(schema.getColumns());
        this.name = new BigInteger(128, new SecureRandom()).toString(32);
        this.schema.setTableName(this.name);
        this.schema.setDataFilePath(schemaManager.getTempRoot() + "/" + this.name);
        this.implementation = new VirtualTable(this.schema, this.name);
        this.isTemproary = true;
    }

    public Table(TableSchema schema, String name) { // names table. Load real data
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

    public void remove(Row row) {
        implementation.remove(row);
    }

    public PagePointer add(Row row) {
        if(row == null) return null;

        if(isVirtual) {
            usedVirtualMemory += row.getRowSize();
            if(usedVirtualMemory > maxVirtualMemory) storeTable();
        }

        return implementation.add(row);
    }

    public void addAll(List<Row> rows) {
        if(isVirtual) {
            for(Row row : rows) usedVirtualMemory += row.getRowSize();
            if(usedVirtualMemory > maxVirtualMemory) storeTable();
        }

        implementation.addAll(rows);
    }

    public void addIndex(Column column, Index index) {
        index.setTable((RealTable) this.implementation); // we can build index only on real table
        indexes.put(column.getName(), index);
    }

    public Index getIndex(Column column) {
        try {
            Index index = indexes.get(column.getName());
            return index;
        } catch (Exception e) {
            return null;
        }
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void waitForUnlock() {
        try{
            mutex.lock();
            condition.await();
            mutex.unlock();
        } catch (Exception e) {
            e.fillInStackTrace();
            System.out.println("Cant wait for unlock");
        }
    }

    public void notifyOne() {
        try{
            mutex.lock();
            condition.signal();
            mutex.unlock();
        } catch (Exception e) {
            e.fillInStackTrace();
            System.out.println("Cant notify one");
        }
    }
}
