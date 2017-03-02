package dbms.query.Operations;

import dbms.index.Index;
import dbms.query.Computator;
import dbms.query.Predicate;
import dbms.storage.table.Table;

import java.util.Stack;

/**
 * Created by alex on 02.03.17.
 */
public class IndexScanOperation implements Operation {
    private Predicate predicate;

    public IndexScanOperation(Predicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public void compute(Computator computator) {
        Stack<Object> computationMachine = computator.getComputationMachine();

        Table table = (Table) computationMachine.pop();
        Object rvalue = computationMachine.pop();
        Object lvalue = computationMachine.pop();

        Index index = table.getIndex(predicate.getColumn());

        try {
            predicate.setAgruments(lvalue, rvalue, table.getSchema());
        } catch (Exception e) {
            e.fillInStackTrace();
            return;
        }

        Table resultTable = index.search(predicate);
        computationMachine.push(resultTable);
        table.clear(); // nothing
    }
}
