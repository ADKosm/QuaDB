package dbms.query;

import dbms.Consts;
import dbms.query.Operations.FullScanOperation;
import dbms.query.Operations.InsertOperation;
import dbms.query.Operations.Operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryPlan {
    private ArrayList<Operation> operations;

    public QueryPlan(String query) {
        operations = new ArrayList<Operation>();

        if(Pattern.matches("select [a-z0-9\\*,]+ from [a-z0-9]+", query)) {
            Matcher m = Pattern.compile("select ([a-z0-9\\*,]+) from ([a-z0-9]+)").matcher(query);
            if(m.find()) {
                String fields = m.group(1); // TODO: add filtering
                String tableName = m.group(2);

                operations.add(new FullScanOperation(tableName, "True")); // TODO: add predicate
            }
        } else if(Pattern.matches("insert into [a-zA-Z0-9]+ values \\(.+\\)", query)) { // TODO: improve regexp
            Matcher m = Pattern.compile("insert into ([a-zA-Z0-9]+) values \\((.+)\\)").matcher(query);
            if(m.find()) {
                String tableName = m.group(1);
                String[] vs = m.group(2).split(",");
                List<String> values = new ArrayList<String>(Arrays.asList(vs));
                operations.add(new InsertOperation(tableName, values));
            }
        }
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
