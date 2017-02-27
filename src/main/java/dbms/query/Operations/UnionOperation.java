package dbms.query.Operations;

import dbms.query.Computator;
import dbms.schema.Row;
import dbms.storage.table.Table;

import java.util.Iterator;
import java.util.Stack;

/**
 * Created by alex on 27.02.17.
 */
public class UnionOperation implements Operation {
    @Override
    public void compute(Computator computator) {
        Stack<Object> computationMachine = computator.getComputationMachine();

        Table one = (Table) computationMachine.pop();
        Table two = (Table) computationMachine.pop();

        Table resultTable = new Table(one.getSchema());

        for(Iterator<Row> iterator = one.iterator(); iterator.hasNext();) {
            resultTable.add(iterator.next());
        }

        for(Iterator<Row> it2 = two.iterator(); it2.hasNext();) { // TODO: consider sorting
            boolean insert = true;
            Row row = it2.next();
            for(Iterator<Row> it1 = one.iterator(); it1.hasNext();) {
                insert = insert && (!row.equals(it1.next()));
            }
            if(insert) {
                resultTable.add(row);
            }
        }

        computationMachine.push(resultTable);
        one.clear();
        two.clear();
    }
}
