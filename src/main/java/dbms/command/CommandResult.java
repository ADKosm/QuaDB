package dbms.command;

import dbms.Consts;
import dbms.query.QueryResult;
import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.Schema;

import java.util.ArrayList;
import java.util.stream.Collectors;

public class CommandResult {
    private long start;
    private int status;
    private float timeSpent;
    private int type;
    private QueryResult queryResult;

    public CommandResult() {
        this.status = Consts.STATUS_COMMAND_UNKNOWN;
        this.start = System.currentTimeMillis();
    }

    public int getStatus() {
        return this.status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public float getTimeSpent(){
        return this.timeSpent;
    }

    public void finishCommand() {
        this.timeSpent = (System.currentTimeMillis() - this.start) / 1000;
    }

    public String toConsoleString() {
        finishCommand();

        String result = "\nResult took " + this.timeSpent + "\n";// + " and " + 0 /*this.queryResult.getRowsNumber()*/ + " rows returned:\n";

        switch (type) {
            case Consts.SHOW_TABLES:
                result += queryResult.getSchemas().stream().map(Schema::getTableName).collect(Collectors.joining("\n"));
                break;
            case Consts.DESCRIBE_TABLE:
                result += queryResult.getSchema().getTableName() + "\n" + queryResult.getSchema().getColumns().stream().map(Column::toString).collect(Collectors.joining("|"));
                break;
            case Consts.SHOW_ROWS:
                break;
        }

        return result;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setQueryResult(QueryResult queryResult) {
        this.queryResult = queryResult;
    }
}
