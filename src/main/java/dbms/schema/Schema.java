package dbms.schema;

import java.util.ArrayList;

public class Schema {
    private ArrayList<Column> columns;
    private String schemaFilePath;
    private String dataFilePath;

    public Schema(ArrayList<Column> columns) {
        this.columns = columns;
    }

    public void setColumns(ArrayList<Column> columns) {
        this.columns = columns;
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }

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
}
