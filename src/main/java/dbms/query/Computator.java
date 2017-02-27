package dbms.query;

import dbms.query.Operations.Operation;
import dbms.storage.table.Table;

import java.util.Iterator;
import java.util.Stack;

public class Computator {
    private QueryResult result;
    private QueryPlan plan;
    private Stack<Object> computationMachine = new Stack<>();

    public Computator(QueryPlan queryPlan) {
        result = new QueryResult();
        plan = queryPlan;
    }

    public void runOperations() {
        Iterator<Object> it = plan.getPlan().iterator();
        while (it.hasNext()) {
            Object current = it.next();
            if(current instanceof Operation) {
                Operation operation = (Operation) current;
                operation.compute(this);
            } else {
                computationMachine.push(current);
            }
        }
        result.setResultTable((Table) computationMachine.pop());
    }

    public QueryResult getResult() {
        return result;
    }

    public Stack<Object> getComputationMachine() {
        return computationMachine;
    }
}
