package dbms.query.Operations;

import dbms.query.Computator;
import dbms.schema.Row;
import dbms.schema.Schema;
import dbms.storage.Page;
import dbms.storage.table.Table;
import javafx.scene.control.Tab;

import java.util.List;
import java.util.Stack;

/**
 * Created by alex on 12.02.17.
 */

/**
 * Stack: |values|table|<top>
 */
public class InsertOperation implements Operation {
    @Override
    public void compute(Computator computator) {
        try{
            Stack<Object> computationMachine = computator.getComputationMachine();

            Table table = (Table) computationMachine.pop();
            List<String> values = (List<String>) computationMachine.pop();

            Row row = table.getSchema().valuesToRow(values);
            table.add(row);

            Table tableWithInsertValue = new Table(table.getSchema());
            tableWithInsertValue.add(row);
            computationMachine.push(tableWithInsertValue);
        } catch (Exception e) {
            e.fillInStackTrace();
        }

    }
}
