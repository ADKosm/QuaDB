package dbms.transaction;


import dbms.Consts;
import dbms.command.CommandResult;
import dbms.query.Computator;
import dbms.schema.Row;
import dbms.schema.dataTypes.PagePointer;
import dbms.storage.StorageManager;
import dbms.storage.table.Table;
import javafx.scene.control.Tab;

import java.io.*;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionManager {
    public static final TransactionManager instance = new TransactionManager();
    public static TransactionManager getInstance() {
        return instance;
    }

    private HashMap<Long, String> currentTransactions = new HashMap<>();
    private StorageManager storageManager = StorageManager.getInstance();

    public CommandResult executeCommand(String command) {
        if(command.equals("begin")) {
            beginTransaction();
        } else if(command.equals("commit")) {
            commitTransaction();
        }
        CommandResult commandResult = new CommandResult();
        commandResult.setStatus(Consts.STATUS_COMMAND_OK);
        commandResult.setType(Consts.SILENCE);
        return commandResult;
    }

    public void recoverDB() {
        // TODO: implement
        Pattern pattern = Pattern.compile(".*\\.transaction$");
        try{
            File[] files = new File(Consts.TRANSACTION_PATH).listFiles();
            for(File file : files) {
                if(pattern.matcher(file.getName()).matches()) {
                    undoTransaction(file);
                }
            }
        } catch (Exception e) {
            System.out.println("Can't open transaction directory");
        }
    }

    public void beginTransaction() {
        // TODO: implement
        String id = new BigInteger(128, new SecureRandom()).toString(32);
        currentTransactions.put(Thread.currentThread().getId(), id);
        File file = new File(Consts.TRANSACTION_PATH + "/" + id + ".transaction");
        try{
            file.createNewFile();
            FileWriter w = new FileWriter(file, true);
            PrintWriter writer = new PrintWriter(w, true);
            writer.println("Begin");
            writer.close();
        } catch (Exception e) {
            e.fillInStackTrace();
        }
    }

    public void commitTransaction() {
        // TODO: implement
        String id = currentTransactions.get(Thread.currentThread().getId());
        File file = new File(Consts.TRANSACTION_PATH + "/" + id + ".transaction");
        try{
            FileWriter w = new FileWriter(file, true);
            PrintWriter writer = new PrintWriter(w, true);
            writer.println("Commit");
            writer.close();
            file.renameTo(new File(Consts.TRANSACTION_PATH + "/" + id + ".transaction.commit"));
        } catch (Exception e) {
            e.fillInStackTrace();
        }
    }

    public void insertValues(PagePointer pointer, Table table) {
        // TODO: implement
        if(!currentTransactions.containsKey(Thread.currentThread().getId())) {
            beginTransaction();
        }
        String id = currentTransactions.get(Thread.currentThread().getId());
        File file = new File(Consts.TRANSACTION_PATH + "/" + id + ".transaction");
        try{
            FileWriter w = new FileWriter(file, true);
            PrintWriter writer = new PrintWriter(w, true);
            writer.println("Write " + table.getName() + " " + pointer.getIndex().toString() + " " + pointer.getOffset().toString());
            writer.close();
        } catch (Exception e) {
            e.fillInStackTrace();
        }
    }

    public void undoTransaction(File file) {
        // TODO: implement
        Pattern pattern = Pattern.compile("Write (.*) ([0-9]+) ([0-9]+)");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String begin = reader.readLine();
            for(String line  = reader.readLine(); line != null; line = reader.readLine()) {
                Matcher m = pattern.matcher(line);
                if(m.find()) {
                    String tableName = m.group(1);
                    Long index = Long.parseLong(m.group(2));
                    Short offset = Short.parseShort(m.group(3));

                    Table table = storageManager.getTable(tableName);
                    Row row = new Row(new ArrayList<>());
                    row.setPagePointer(new PagePointer(index, offset));

                    table.remove(row);
                }
            }
        } catch (Exception e) {
            e.fillInStackTrace();
        }

    }
}
