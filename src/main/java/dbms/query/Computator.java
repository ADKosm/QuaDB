package dbms.query;

import dbms.query.Operations.FullScanOperation;
import dbms.query.Operations.InsertOperation;
import dbms.query.Operations.Operation;
import dbms.schema.Row;
import dbms.schema.Schema;
import dbms.schema.SchemaManager;
import dbms.storage.BufferManager;
import dbms.storage.DiskManager;
import dbms.storage.Page;

import java.util.List;

public class Computator {
    private QueryResult result;

    private SchemaManager schemaManager = SchemaManager.getInstance();
    private BufferManager bufferManager = BufferManager.getInstance();
    private DiskManager diskManager = DiskManager.getInstance();

    public Computator() {
        result = new QueryResult();
    }

    public void compute(Operation operation) {
        if(operation instanceof FullScanOperation) {
            FullScanOperation op = (FullScanOperation) operation;
            fullScan(op.getEntityName(), op.getPredicate());
        } else if (operation instanceof InsertOperation) {
            InsertOperation op = (InsertOperation) operation;
            insertValues(op.getEntityName(), op.getValues());
        }
    }

    public QueryResult getResult() {
        return result;
    }

    public void setResult(QueryResult result) {
        this.result = result;
    }


    private void fullScan(String table, String predicate) {
        Schema schema = schemaManager.getSchema(table);

        Page page;
        for(int i = 0; (page = bufferManager.getPage(schema.getDataFilePath(), i)) != null; i++) {
            List<Row> rows = page.toRows(schema);
            result.getResults().addAll(rows); // TODO: add checking predicate
        }
    }

    private void insertValues(String table, List<String> values) {
        try{
            Schema schema = schemaManager.getSchema(table);
            Row row = schema.valuesToRow(values);

            Page page = bufferManager.getLastPage(schema.getDataFilePath());

            if(page == null || !page.canPlaced(row)) {
                page = bufferManager.allocateNewPage(schema);
            }

            page.insertValues(row);

            result.getResults().add(row);
        } catch (Exception e) {
            e.fillInStackTrace();
        }

    }
}
