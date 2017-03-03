package dbms.query.Operations;

import dbms.query.Computator;
import dbms.query.Predicate;
import dbms.schema.Row;
import dbms.storage.table.Table;

import java.util.Iterator;
import java.util.Stack;

/**
 * Stack: |lvalue|rvalue|table|<top>
 */
public class FullScanOperation implements Operation {
    private String entityName;
    private Predicate predicate;

    public FullScanOperation(Predicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public void compute(Computator computator) {
        Stack<Object> computationMachine = computator.getComputationMachine();

        Table table = (Table) computationMachine.pop();
        Object rvalue = computationMachine.pop();
        Object lvalue = computationMachine.pop();

        try {
            predicate.setAgruments(lvalue, rvalue, table.getSchema());
        } catch (Exception e) {
            e.fillInStackTrace();
            return;
        }

        Table resultTable = new Table(table.getSchema());
        Iterator<Row> rowIterator = table.iterator();
        while(rowIterator.hasNext()) {
            Row row = rowIterator.next();
            if(predicate.check(row)) {
                resultTable.add(row);
            }
        }
        computationMachine.push(resultTable);
        table.clear();
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }
}
