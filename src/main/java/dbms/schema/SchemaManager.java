package dbms.schema;

import dbms.Consts;
import dbms.command.CommandResult;

public class SchemaManager {

    private String shemaRoot;

    public SchemaManager() {
        // TODO load schema to RAM
        this.shemaRoot = Consts.SCHEMA_ROOT_PATH;
    }

    public CommandResult executeCommand(String query) {
        CommandResult commandResult = new CommandResult();
        commandResult.setStatus(Consts.STATUS_COMMAND_OK);
        return commandResult;
    }
}
