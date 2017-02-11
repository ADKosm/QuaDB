package dbms.schema;

import dbms.Consts;
import dbms.command.CommandResult;
import dbms.query.QueryResult;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaManager {

    private String shemaRoot;
    private HashMap<String, Schema> structure;
    private HashMap<String, Integer> typesMap;
    private Pattern metaPattern;

    private void loadScheme(File table) throws Exception {
        ArrayList<Column> colums = new ArrayList<Column>();
        BufferedReader br = new BufferedReader(new FileReader(table));
        for(String line = br.readLine(); line != null; line = br.readLine()) {
            String[] data = line.split(";");
            colums.add(
                    data.length == 3 ?
                    new Column(data[0], typesMap.get(data[1]), Integer.parseInt(data[2])) :
                    new Column(data[0], typesMap.get(data[1]))
            );
        }
        Schema schema = new Schema(colums);
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
    }

    public SchemaManager() {
        // TODO load schema to RAM
        structure = new HashMap<String, Schema>();
        typesMap = new HashMap<String, Integer>();
        typesMap.put("int", Consts.COLUMN_TYPE_INTEGER);
        typesMap.put("varchar", Consts.COLUMN_TYPE_VARCHAR);
        typesMap.put("datetime", Consts.COLUMN_TYPE_DATETIME);

        this.shemaRoot = Consts.SCHEMA_ROOT_PATH;
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
            result.setSchemas(new ArrayList<Schema>(structure.values()));
            commandResult.setQueryResult(result);
            commandResult.setType(Consts.SHOW_TABLES);
        } else if(Pattern.matches("describe table [a-z0-9]+", query)) {
            QueryResult result = new QueryResult();
            Matcher m = Pattern.compile("describe table ([a-z0-9]+)").matcher(query);
            if(m.find()) {
                Schema schema = structure.get(m.group(1));
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
}
