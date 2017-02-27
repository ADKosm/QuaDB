package dbms.query;

import dbms.schema.Row;
import dbms.schema.Schema;
import dbms.storage.table.Table;

import java.util.ArrayList;
import java.util.Collection;

public class QueryResult {
    private ArrayList<Schema> schemas = new ArrayList<Schema>();
    private Schema schema;
    private ArrayList<Row> results = new ArrayList<Row>();
    private Table resultTable;
    private int rowsNumber = 0;

    public void setResults(ArrayList<Row> results) {
        this.results = results;
    }

    public ArrayList<Row> getResults() {
        return results;
    }

    public void setSchemas(ArrayList<Schema> schemas) {
        this.schemas = schemas;
    }

    public ArrayList<Schema> getSchemas() {
        return schemas;
    }

    public void setRowsNumber(int rowsNumber) {
        this.rowsNumber = rowsNumber;
    }

    public int getRowsNumber() {
        return rowsNumber;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Table getResultTable() {
        return resultTable;
    }

    public void setResultTable(Table resultTable) {
        this.resultTable = resultTable;
    }
}
