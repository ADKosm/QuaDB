package dbms.command;

import dbms.Consts;

import java.util.regex.Pattern;

public class CommandManager {
    private String command;
    private int commandStatus;

    public CommandManager(String command){
        this.command = command.toLowerCase();
        this.commandStatus = Consts.UNKNOWN_COMMAND;

        for(Pattern pattern : Consts.CONTROL_COMMANDS) {
            if (pattern.matcher(this.command).matches()){
                this.commandStatus = Consts.CONTROL_COMMAND;
                break;
            }
        }
        for(Pattern pattern : Consts.SCHEMA_COMMANDS) {
            if (pattern.matcher(this.command).matches()){
                this.commandStatus = Consts.DDL_COMMAND;
                break;
            }
        }
        for(Pattern pattern : Consts.QUERY_COMMANDS) {
            if (pattern.matcher(this.command).matches()){
                this.commandStatus = Consts.DML_COMMAND;
                break;
            }
        }
    }

    public int getCommandType(){
        return this.commandStatus;
    }

    public CommandResult executeCommand(){
        CommandResult commandResult = new CommandResult();
        if (this.command.equals(Consts.COMMAND_EXIT)) {
            commandResult.setStatus(Consts.STATUS_COMMAND_EXIT);
        }
        return commandResult;
    }
}
