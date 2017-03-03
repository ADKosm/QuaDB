package dbms.schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by alex on 02.03.17.
 */
public abstract class Schema {
    private String schemaFilePath;
    private String dataFilePath;
    private String tableName;
    protected List<Column> columns = new ArrayList<>();


    public void setDataFilePath(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    public String getDataFilePath() {
        return dataFilePath;
    }

    public void setSchemaFilePath(String schemaFilePath) {
        this.schemaFilePath = schemaFilePath;
    }

    public String getSchemaFilePath() {
        return schemaFilePath;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
