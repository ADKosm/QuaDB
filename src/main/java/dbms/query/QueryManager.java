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
        BufferManager bufferManager = BufferManager.getInstance();

        commandResult.setQueryResult(bufferManager.executeQuery(queryPlan));
        commandResult.setType(Consts.SHOW_ROWS);
        return commandResult;
    }
}
