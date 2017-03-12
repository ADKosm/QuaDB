package dbms.query;

import dbms.query.Operations.*;
import dbms.schema.Column;
import dbms.schema.TableSchema;
import dbms.storage.StorageManager;
import dbms.storage.table.Table;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryPlan {
    private StorageManager storageManager = StorageManager.getInstance();
    private List<Object> plan = new ArrayList<>();

    public QueryPlan(String query) {
        if(Pattern.matches("select [a-z0-9\\*,]+ from [a-z0-9]+( where .+)*", query)) {
            Matcher m = Pattern.compile("select ([a-z0-9\\*,]+) from ([a-z0-9]+)( where (.+))*").matcher(query);
            if(m.find()) {
                String fields = m.group(1); // TODO: add filtering
                String tableName = m.group(2);
                String rawPredicate = m.group(4) == null ? "True" : m.group(4);

                buildSelectPlan(rawPredicate, tableName);
            }
        } else if(Pattern.matches("insert into [a-zA-Z0-9]+ values \\(.+\\)", query)) { // TODO: improve regexp
            Matcher m = Pattern.compile("insert into ([a-zA-Z0-9]+) values \\((.+)\\)").matcher(query);
            if(m.find()) {
                String tableName = m.group(1);
                String[] vs = m.group(2).split(",");
                List<String> values = new ArrayList<String>(Arrays.asList(vs));

                buildInsertPlan(values, tableName);
            }
        } else if(Pattern.matches("delete from [a-z0-9]+( where .+)*", query)) {
            Matcher m = Pattern.compile("delete from ([a-z0-9]+)( where (.+))*").matcher(query);
            if(m.find()) {
                String tableName = m.group(1);
                String rawPredicate = m.group(3) == null ? "True" : m.group(3);

                buildDeletePlan(rawPredicate, tableName);
            }
        }
    }

    public List<Object> getPlan() {
        return plan;
    }

    private void buildDeletePlan(String rawPredicate, String tableName) {
        buildSelectPlan(rawPredicate, tableName);

        Table table = storageManager.getTable(tableName);
        plan.add(table);

        plan.add(new DeleteOperation());
    }

    private void buildSelectPlan(String rawPredicate, String tableName) {
        Pattern lexemPattern = Pattern.compile("(and|\\(|\\)|<|>|=|or|[0-9]+|[A-Za-z0-9_]+)\\s*");
        List<String> lexems = new ArrayList<>();
        Matcher m = lexemPattern.matcher(rawPredicate);
        while (m.find()) lexems.add(m.group(1));

        Table table = storageManager.getTable(tableName);

        ShuntingYard parser = new ShuntingYard(lexems, table);
        parser.run();

        optimizePlan();
    }

    private void optimizePlan() {
        // TODO: optimize query plan using relation algebra and indexes
        PlanOptimizer planOptimizer = new PlanOptimizer();
        planOptimizer.precomputeConstantPredicateStatement();
        planOptimizer.useIndexSearch();
    }

    private void buildInsertPlan(List<String> values, String tableName) {
        plan.add(values);
        plan.add(storageManager.getTable(tableName));
        plan.add(new InsertOperation());
    }


    public class ShuntingYard {
        private HashMap<String, Integer> priority = new HashMap<>();
        Stack<String> operators = new Stack<>();

        private List<String> lexems;
        private Table table;

        public ShuntingYard(List<String> lexems, Table table) {
            this.lexems = lexems;
            this.table = table;

            priority.put("(", 0);
            priority.put(")", 0);
            priority.put("and", 1);
            priority.put("or", 1);
            priority.put("<", 2);
            priority.put(">", 2);
            priority.put("=", 2);
            priority.put("True", 2);
            priority.put("False", 2);
        }

        public void run() {
            Iterator<String> it = lexems.iterator();
            while (it.hasNext()) {
                String current = it.next();

                if(priority.containsKey(current)) { // operator or bracket
                    if(current.equals("(")) {
                        operators.push(current);
                        continue;
                    }
                    if(current.equals(")")) {
                        while(!operators.peek().equals("(")) {
                            addOperator(operators.pop());
                        }
                        operators.pop();
                        continue;
                    }

                    while (!operators.empty() && priority.get(current) <= priority.get(operators.peek())) { // while proiroty is greater
                        addOperator(operators.pop());
                    }
                    operators.push(current);
                } else { // number or column
                    addValue(current);
                }
            }
            while (!operators.empty()) {
                addOperator(operators.pop());
            }
        }

        private void addOperator(String operator) {
            if(priority.get(operator) == 2) { // < > = True False
                plan.add(table);
                plan.add(new FullScanOperation(new Predicate(operator)));
            }
            if(operator.equals("and")) {
                plan.add(new IntersectionOperation());
            }
            if(operator.equals("or")) {
                plan.add(new UnionOperation());
            }
        }

        private void addValue(String value) {
            try {
                Column column = table.getSchema().getColumn(value);
                plan.add(column);
            } catch (Exception e) {
                plan.add(value);
            }
        }
    }

    public class PlanOptimizer {
        public void precomputeConstantPredicateStatement() {
            for(int i = 0; i < plan.size(); i++) {
                if(plan.get(i) instanceof FullScanOperation) {
                    FullScanOperation scan = (FullScanOperation) plan.get(i);
                    if(scan.getPredicate().getOperator().equals("True")) {
                        plan.remove(i);
                        i--;
                        continue;
                    }
                    if(scan.getPredicate().getOperator().equals("False")) {
                        TableSchema schema = ((Table) plan.get(i-1)).getSchema();
                        plan.set(i-1, new Table(schema)); // empty table
                        plan.remove(i);
                        i--;
                        continue;
                    }
                }
            }
        }

        public void useIndexSearch() {
            if(plan.size() < 4) return;
            for(int i = 3; i < plan.size(); i++) {
                if(plan.get(i) instanceof FullScanOperation) {
                    Table table = (Table)plan.get(i-1);
                    Object lv = plan.get(i-2);
                    Object rv = plan.get(i-3);
                    if((lv instanceof Column && !(rv instanceof Column))
                            || (rv instanceof Column && !(lv instanceof Column))) {
                        Column c = (Column)(lv instanceof Column ? lv : rv);
                        if(table.getIndex(c) != null) {
                            plan.set(i, new IndexScanOperation(((FullScanOperation) plan.get(i)).getPredicate()));
                        }
                    }


                }
            }
        }
    }
}

