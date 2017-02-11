package dbms.query;

import dbms.schema.Row;
import dbms.schema.Schema;

import java.util.ArrayList;

public class QueryResult {
    private ArrayList<Schema> schema = new ArrayList<Schema>();
    private ArrayList<Row> results = new ArrayList<Row>();
    private int rowsNumber = 0;

    public void setResults(ArrayList<Row> results) {
        this.results = results;
    }

    public ArrayList<Row> getResults() {
        return results;
    }

    public void setSchema(ArrayList<Schema> schema) {
        this.schema = schema;
    }

    public ArrayList<Schema> getSchema() {
        return schema;
    }

    public void setRowsNumber(int rowsNumber) {
        this.rowsNumber = rowsNumber;
    }

    public int getRowsNumber() {
        return rowsNumber;
    }
}
