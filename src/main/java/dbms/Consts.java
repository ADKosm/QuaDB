package dbms;

import dbms.schema.dataTypes.Int;

import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class Consts {

    // Database Server options
    public static final int PORT = 8000;
    public static final String SCHEMA_ROOT_PATH = "/tmp/simpledb";
    public static final String SCHEMA_INDEX_PATH = "/tmp/simpledb/indexes";
    public static final String TRANSACTION_PATH = "/tmp/simpledb/transactions";

    // Database options
    public static final int BLOCK_SIZE = 4096;

    // Commands
    public static final String COMMAND_EXIT = "exit";
    public static final List<Pattern> CONTROL_COMMANDS = Collections.singletonList(Pattern.compile(COMMAND_EXIT));
    public static final List<Pattern> SCHEMA_COMMANDS =
            Arrays.asList(
                    Pattern.compile("show tables"),
                    Pattern.compile("describe table [a-z0-9]+")
            );
    public static final List<Pattern> QUERY_COMMANDS =
            Arrays.asList(
                    Pattern.compile("select [a-z0-9\\*,]+ from [a-z0-9]+( where .+)*"),
                    Pattern.compile("insert into [a-zA-Z0-9]+ values \\(.+\\)"),
                    Pattern.compile("delete from [a-z0-9]+( where .+)*")
            );
    public static final List<Pattern> TRANSACTION_COMMANDS =
            Arrays.asList(
                    Pattern.compile("begin"),
                    Pattern.compile("commit")
            );

    public static final Integer SHARED_LOCK = 0;
    public static final Integer EXCLUSIVE_LOCK = 1;

    public static final int CONTROL_COMMAND = 0;
    public static final int DDL_COMMAND = 1;
    public static final int DML_COMMAND = 2;
    public static final int TRANSACTION_COMMAND = 3;
    public static final int UNKNOWN_COMMAND = -1;

    public static final int SHOW_TABLES = 0;
    public static final int DESCRIBE_TABLE = 1;
    public static final int SHOW_ROWS = 2;
    public static final int SILENCE = 3;

    public static final int STATUS_COMMAND_UNKNOWN = -1;
    public static final int STATUS_COMMAND_EXIT = -2;
    public static final int STATUS_COMMAND_OK = 0;
    public static final int STATUS_COMMAND_ERROR = 1;

    // Operations
    public static final int FULL_SCAN = 0;

    // Messages
    public static final String MESSAGE_WARNING_UNKNOWN_COMMAND = "Unknown command: ";
    public static final String MESSAGE_WARNING_INVALID_COMMAND = "Invalid command: ";
    public static final String MESSAGE_WARNING_INVALID_QUERY = "Invalid query: ";
    public static final String MESSAGE_BYE = "See you later\n";

    // Column types
    public static final int COLUMN_TYPE_INTEGER = 1;
    public static final int COLUMN_TYPE_VARCHAR = 2;
    public static final int COLUMN_TYPE_DATETIME = 3;
    public static final int COLUMN_TYPE_POINTER = 4;
    public static final int COLUMN_TYPE_PAGEPOINTER = 5;
    public enum COLUMN_TYPES {COLUMN_TYPE_INTEGER, COLUMN_TYPE_VARCHAR, COLUMN_TYPE_DATETIME, COLUMN_TYPE_POINTER, COLUMN_TYPE_PAGEPOINTER};

    public static final int MAX_MEMORY_USED = 4096 * 2; // 2 pages | TODO: increase in future
}