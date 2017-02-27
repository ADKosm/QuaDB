package dbms.storage;

import dbms.schema.Schema;
import dbms.storage.table.Table;

import java.util.HashMap;

/**
 * Created by alex on 27.02.17.
 */
public class StorageManager {
    private static StorageManager ourInstance = new StorageManager();
    public static StorageManager getInstance() {
        return ourInstance;
    }

    private HashMap<String, Table> tables = new HashMap<String, Table>(); // tableName -> Table

    private StorageManager() {
    }

    public void loadTable(Schema schema) {
        tables.put(schema.getTableName(), new Table(schema, schema.getTableName()));
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

}
