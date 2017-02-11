package dbms.query;

import dbms.Consts;
import dbms.command.CommandResult;
import dbms.storage.BufferManager;

public class QueryManager {

    public CommandResult executeCommand(String query) {
        QueryPlan queryPlan = new QueryPlan(query);
        BufferManager bufferManager = new BufferManager();
        bufferManager.executeQuery(queryPlan);

        CommandResult commandResult = new CommandResult();
        commandResult.setStatus(Consts.STATUS_COMMAND_OK);
        return commandResult;
    }
}
