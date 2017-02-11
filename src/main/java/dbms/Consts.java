package dbms;

import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class Consts {

    // Database Server options
    public static final int PORT = 8000;
    public static final String SCHEMA_ROOT_PATH = "/tmp/simpledb";

    // Database options
    public static final int BLOCK_SIZE = 4096;

    // Commands
    public static final String COMMAND_EXIT = "exit";
    public static final List<Pattern> CONTROL_COMMANDS = Collections.singletonList(Pattern.compile(COMMAND_EXIT));
    public static final List<Pattern> SCHEMA_COMMANDS =
            Arrays.asList(Pattern.compile("show tables"), Pattern.compile("describe table [a-z0-9]+"));
    public static final List<Pattern> QUERY_COMMANDS =
            Arrays.asList(Pattern.compile("select [a-z0-9\\*,]+ from [a-z0-9]+"));

    public static final Integer CONTROL_COMMAND = 0;
    public static final Integer DDL_COMMAND = 1;
    public static final Integer DML_COMMAND = 2;
    public static final Integer UNKNOWN_COMMAND = -1;

    public static final int STATUS_COMMAND_UNKNOWN = -1;
    public static final int STATUS_COMMAND_EXIT = -2;
    public static final int STATUS_COMMAND_OK = 0;
    public static final int STATUS_COMMAND_ERROR = 1;

    // Messages
    public static final String MESSAGE_WARNING_UNKNOWN_COMMAND = "Unknown command: ";
    public static final String MESSAGE_WARNING_INVALID_COMMAND = "Invalid command: ";
    public static final String MESSAGE_WARNING_INVALID_QUERY = "Invalid query: ";
    public static final String MESSAGE_BYE = "See you later\n";

    // Column types
    public static final int COLUMN_TYPE_INTEGER = 1;
    public static final int COLUMN_TYPE_VARCHAR = 2;
    public static final int COLUMN_TYPE_DATETIME = 3;
    public enum COLUMN_TYPES {COLUMN_TYPE_INTEGER, COLUMN_TYPE_VARCHAR, COLUMN_TYPE_DATETIME};
}