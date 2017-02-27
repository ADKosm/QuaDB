package dbms.query;

import dbms.Consts;
import dbms.command.CommandResult;
import dbms.storage.BufferManager;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryManager {

    public CommandResult executeCommand(String query) {
        CommandResult commandResult = new CommandResult();
        commandResult.setStatus(Consts.STATUS_COMMAND_OK);

        QueryPlan queryPlan = new QueryPlan(query);

        QueryResult result = executeQuery(queryPlan);

        commandResult.setQueryResult(result);
        commandResult.setType(Consts.SHOW_ROWS);
        return commandResult;
    }

    public QueryResult executeQuery(QueryPlan queryPlan) {
        Computator computator = new Computator(queryPlan);
        computator.runOperations();
        return computator.getResult();
    }
}
