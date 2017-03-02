package dbms.query;

import dbms.schema.Row;
import dbms.schema.TableSchema;
import dbms.storage.table.Table;

import java.util.ArrayList;

public class QueryResult {
    private ArrayList<TableSchema> schemas = new ArrayList<TableSchema>();
    private TableSchema schema;
    private ArrayList<Row> results = new ArrayList<Row>();
    private Table resultTable;
    private int rowsNumber = 0;

    public void setResults(ArrayList<Row> results) {
        this.results = results;
    }

    public ArrayList<Row> getResults() {
        return results;
    }

    public void setSchemas(ArrayList<TableSchema> schemas) {
        this.schemas = schemas;
    }

    public ArrayList<TableSchema> getSchemas() {
        return schemas;
    }

    public void setRowsNumber(int rowsNumber) {
        this.rowsNumber = rowsNumber;
    }

    public int getRowsNumber() {
        return rowsNumber;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }

    public Table getResultTable() {
        return resultTable;
    }

    public void setResultTable(Table resultTable) {
        this.resultTable = resultTable;
    }
}
