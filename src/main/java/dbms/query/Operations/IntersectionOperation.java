package dbms.query.Operations;

import dbms.query.Computator;
import dbms.schema.Row;
import dbms.storage.table.Table;
import javafx.scene.control.Tab;

import java.util.Iterator;
import java.util.Stack;

/**
 * Created by alex on 27.02.17.
 */
public class IntersectionOperation implements Operation {
    @Override
    public void compute(Computator computator) {
        Stack<Object> computationMachine = computator.getComputationMachine();

        Table one = (Table) computationMachine.pop();
        Table two = (Table) computationMachine.pop();

        Table resultTable = new Table(one.getSchema());

        for(Iterator<Row> it1 = one.iterator(); it1.hasNext();) { // TODO: consider sorting
            Row row1 = it1.next();
            for(Iterator<Row> it2 = two.iterator(); it2.hasNext();) {
                Row row2 = it2.next();
                if(row1.equals(row2)) {
                    resultTable.add(row1);
                    break;
                }
            }
        }
        computationMachine.push(resultTable);
        one.clear();
        two.clear();
    }
}
