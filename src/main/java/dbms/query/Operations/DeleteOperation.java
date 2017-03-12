package dbms.query.Operations;

import dbms.query.Computator;
import dbms.schema.Row;
import dbms.storage.table.Table;

import java.util.Iterator;
import java.util.Stack;

/**
 * Created by alex on 12.03.17.
 */
public class DeleteOperation implements Operation{
    @Override
    public void compute(Computator computator) {
        Stack<Object> computationMachine = computator.getComputationMachine();

        Table targetTable = (Table) computationMachine.pop();
        Table tableOfDeads = (Table) computationMachine.pop();

        Iterator<Row> rowIterator = tableOfDeads.iterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            targetTable.remove(row);
        }

        computationMachine.push(tableOfDeads); // clear after input
    }
}
