package dbms.query;

import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.Schema;
import dbms.schema.dataTypes.Cell;

import java.util.HashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by alex on 27.02.17.
 */
public class Predicate {
    private String operator;
    Function<Integer, Boolean> executableOperator;

    private boolean leftColumn;
    private boolean rightColumn;

    private Object leftValue;
    private Object rightValue;

    public Predicate(String operator) {
        this.operator = operator;
        switch (operator) {
            case ">":
                executableOperator = (Integer d) -> d > 0;
                break;
            case "<":
                executableOperator = (Integer d) -> d < 0;
                break;
            case "=":
                executableOperator = (Integer d) -> d == 0;
                break;
        }
    }

    public String getOperator() {
        return operator;
    }

    public void setAgruments(Object lvalue, Object rvalue, Schema schema) throws Exception{
        if(lvalue instanceof Column) {
            leftColumn = true;
            leftValue = (Integer) schema.getColumns().indexOf((Column) lvalue);
            if((int)leftValue < 0) throw new Exception("Incorrect left column");
        } else {
            leftColumn = false;
            leftValue = lvalue;
        }

        if(rvalue instanceof Column) {
            rightColumn = true;
            rightValue = (Integer) schema.getColumns().indexOf((Column) rvalue);
            if((int)rightValue < 0) throw new Exception("Incorrect right column");
        } else {
            rightColumn = false;
            rightValue = rvalue;
        }
    }

    public boolean check(Row row) {
        if(leftColumn) {
            Cell lCol = row.getCells().get((Integer) leftValue);
            if(rightColumn) {
                Cell rCol = row.getCells().get((Integer) rightValue);
                return executableOperator.apply(lCol.compareTo(rCol));
            } else {
                String rVal = (String) rightValue;
                return executableOperator.apply(lCol.compareTo(rVal));
            }
        } else {
            String lVal = (String) leftValue;
            if(rightColumn) {
                Cell rCol = row.getCells().get((Integer) rightValue);
                return executableOperator.apply(rCol.compareTo(lVal)*(-1));
            } else { // strange situation
                String rVal = (String) rightValue;
                return executableOperator.apply(lVal.compareTo(rVal)); // TODO: fix
            }
        }
    }
}
