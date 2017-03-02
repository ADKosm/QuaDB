package dbms.schema;

import dbms.Consts;
import dbms.command.CommandResult;
import dbms.query.QueryResult;
import dbms.storage.StorageManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaManager { // TODO: доделать
    public static final SchemaManager instance = new SchemaManager();
    public static SchemaManager getInstance() { return instance; }

    private StorageManager storageManager = StorageManager.getInstance();

    private String shemaRoot;
    private String tempRoot;
    private String indexRoot;
    private HashMap<String, TableSchema> structure; // TODO: replace with interface
    private HashMap<String, Integer> typesMap;
    private Pattern metaPattern;

    public TableSchema getSchema(String tableName) {
        return structure.get(tableName);
    }

    public String getIndexRoot() {
        return indexRoot;
    }

    private void loadScheme(File table) throws Exception {
        ArrayList<Column> colums = new ArrayList<Column>();
        BufferedReader br = new BufferedReader(new FileReader(table));
        for(String line = br.readLine(); line != null; line = br.readLine()) {
            String[] data = line.split(";");
            colums.add(
                    data.length == 3 ?
                    new Column(data[0], typesMap.get(data[1]), Byte.parseByte(data[2])) :
                    new Column(data[0], typesMap.get(data[1]))
            );
        }
        TableSchema schema = new TableSchema(colums);
        Matcher m = Pattern.compile("([a-zA-Z0-9]+)\\.meta").matcher(table.getName());
        String tableName;
        if(m.find()) {
            tableName = m.group(1);
            schema.setTableName(tableName);
            schema.setDataFilePath(shemaRoot + "/" + tableName + ".data");
            schema.setSchemaFilePath(shemaRoot + "/" + tableName + ".meta");
        } else {
            throw new Exception("Impossible situation");
        }
        structure.put(tableName, schema);

        storageManager.loadTable(schema); // TODO: load indexes
    }

    public SchemaManager() {
        structure = new HashMap<String, TableSchema>();
        typesMap = new HashMap<String, Integer>();
        typesMap.put("int", Consts.COLUMN_TYPE_INTEGER);
        typesMap.put("varchar", Consts.COLUMN_TYPE_VARCHAR);
        typesMap.put("datetime", Consts.COLUMN_TYPE_DATETIME);

        this.shemaRoot = Consts.SCHEMA_ROOT_PATH;
        this.tempRoot = this.shemaRoot + "/.qua_temp";
        this.indexRoot = this.shemaRoot + "/indexes";

        for(String dir : Arrays.asList(this.tempRoot, this.indexRoot)){
            File tempDir = new File(dir);
            if(!tempDir.exists()) {
                try {
                    tempDir.mkdir();
                } catch (Exception e) {
                    e.fillInStackTrace();
                }
            }
        }

        metaPattern = Pattern.compile("[a-zA-Z0-9]+\\.meta$");

        try{
            File[] files = new File(this.shemaRoot).listFiles();
            for(File file : files) {
                if(this.metaPattern.matcher(file.getName()).matches()) {
                    loadScheme(file);
                }
            }
        } catch (Exception e) {
            System.out.println("Can't open schema directory");
        }
    }

    public CommandResult executeCommand(String query) throws Exception {
        CommandResult commandResult = new CommandResult();
        commandResult.setStatus(Consts.STATUS_COMMAND_OK);

        if(Pattern.matches("show tables", query)) {
            QueryResult result = new QueryResult();
            result.setSchemas(new ArrayList<TableSchema>(structure.values()));
            commandResult.setQueryResult(result);
            commandResult.setType(Consts.SHOW_TABLES);
        } else if(Pattern.matches("describe table [a-z0-9]+", query)) {
            QueryResult result = new QueryResult();
            Matcher m = Pattern.compile("describe table ([a-z0-9]+)").matcher(query);
            if(m.find()) {
                TableSchema schema = structure.get(m.group(1));
                if(schema == null) {
                    throw new Exception("Can't describe this table");
                }
                result.setSchema(schema);
                commandResult.setQueryResult(result);
                commandResult.setType(Consts.DESCRIBE_TABLE);
            }
        } else {
            throw new Exception("Bad query");
        }

        return commandResult;
    }

    public String getTempRoot() {
        return tempRoot;
    }

    public void setTempRoot(String tempRoot) {
        this.tempRoot = tempRoot;
    }
}
