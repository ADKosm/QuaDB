package dbms.query.Operations;

import dbms.index.Index;
import dbms.query.Computator;
import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.dataTypes.PagePointer;
import dbms.storage.table.Table;

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
            PagePointer pointer = table.add(row);

            for(Column column: table.getSchema().getColumns()) {
                Index index = table.getIndex(column);
                if(index != null) {
                    index.add(row, pointer);
                }
            }

            Table tableWithInsertValue = new Table(table.getSchema());
            tableWithInsertValue.add(row);
            computationMachine.push(tableWithInsertValue);
        } catch (Exception e) {
            e.fillInStackTrace();
        }

    }
}
