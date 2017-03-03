package dbms.storage;

import dbms.Consts;
import dbms.index.Index;
import dbms.index.IndexSchema;
import dbms.schema.Column;
import dbms.schema.Schema;
import dbms.schema.SchemaManager;
import dbms.schema.TableSchema;
import dbms.storage.table.Table;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

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

    public void loadTable(TableSchema schema) {
        Table newTable = new Table(schema, schema.getTableName());
        loadIndexes(newTable);
        tables.put(schema.getTableName(), newTable);
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public void loadIndexes(Table table) {
        for(Column column : table.getSchema().getColumns()) {
            String path = Consts.SCHEMA_INDEX_PATH + "/" + table.getName() + "_" + column.getName();
            try{
                BufferedReader indexFile = new BufferedReader(new FileReader(new File(
                         path + ".meta"
                )));
                Long rootPage = Long.parseLong(indexFile.readLine());

                IndexSchema indexSchema = new IndexSchema(column);
                indexSchema.setDataFilePath(path + ".index"); // must exist!
                indexSchema.setSchemaFilePath(path + ".meta");
                indexSchema.setTableName(table.getName());
                indexSchema.setRootPosition(rootPage);

                Index newIndex = new Index(indexSchema);
                table.addIndex(column, newIndex);
            } catch (Exception e) {
                e.fillInStackTrace();
                continue;
            }
        }
    }

    public void updateIndexMeta(IndexSchema indexSchema) {
        try {
//            BufferedWriter indexFile = new BufferedWriter(new FileWriter(new File(
//                indexSchema.getSchemaFilePath()
//            ), false));
            RandomAccessFile indexFile = new RandomAccessFile(indexSchema.getSchemaFilePath(), "rw");
            indexFile.setLength(0);
            indexFile.writeBytes(indexSchema.serialize());
        } catch (Exception e) {
            e.fillInStackTrace();
        }
    }
}
