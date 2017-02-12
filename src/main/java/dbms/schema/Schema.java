package dbms.schema;

import dbms.schema.dataTypes.Cell;

import java.util.ArrayList;
import java.util.List;

public class Schema {
    private ArrayList<Column> columns;
    private String schemaFilePath;
    private String dataFilePath;
    private String tableName;

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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getRowSize() {
        Integer result = 0;
        for(Column column : columns) {
            result += column.getByteSize();
        }
        result += 1; // byte for alive flag
        return result;
    }

    public Row valuesToRow(List<String> values) throws Exception {
        if(values.size() != columns.size()) {
            throw new Exception("Incorrect shape of values"); // TODO: add NULL as value
        }
        List<Cell> cells = new ArrayList<Cell>();
        for(int i = 0; i < values.size(); i++) {
            cells.add(columns.get(i).createCell(values.get(i)));
        }
        return new Row(cells);
    }
}
