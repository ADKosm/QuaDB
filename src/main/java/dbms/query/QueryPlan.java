package dbms.query;

import java.util.ArrayList;

public class QueryPlan {
    private ArrayList<Operation> operations;

    public QueryPlan(String query) {

    }

    public void setOperations(ArrayList<Operation> operations) {
        this.operations = operations;
    }

    public ArrayList<Operation> getOperations() {
        return operations;
    }

    private void addOperation(Operation operation) {
        this.operations.add(operation);
    }
}
