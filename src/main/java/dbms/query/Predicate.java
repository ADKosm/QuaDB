package dbms.query;

import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.TableSchema;
import dbms.schema.dataTypes.Cell;

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

    private Object lval;
    private Object rval;

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

    // TODO: refactor

    public Column getColumn() {
        if(lval instanceof Column) return (Column) lval;
        if(rval instanceof Column) return (Column) rval;
        return null;
    }

    public Object getValue() {
        if(lval instanceof Column && !(rval instanceof Column)) return rval;
        if(rval instanceof Column && !(lval instanceof Column)) return lval;
        return null;
    }

    public String getOperator() {
        return operator;
    }

    public void setAgruments(Object lvalue, Object rvalue, TableSchema schema) throws Exception{
        lval = lvalue;
        rval = rvalue;
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
