package dbms.transaction;


import dbms.Consts;
import dbms.command.CommandResult;
import dbms.query.Computator;
import dbms.query.QueryManager;
import dbms.schema.Row;
import dbms.schema.dataTypes.PagePointer;
import dbms.storage.StorageManager;
import dbms.storage.table.Table;
import javafx.scene.control.Tab;

import java.io.*;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionManager {
    public static final TransactionManager instance = new TransactionManager();
    public static TransactionManager getInstance() {
        return instance;
    }

    private Map<Long, String> currentTransactions = new ConcurrentHashMap<>();
    private Map<Long, List<LockInfo> > lockQueries = new ConcurrentHashMap<>();
    private Map<Long, List<String> > transactionOperation = new ConcurrentHashMap<>(); // TODO: change to concurrent hash map
    private StorageManager storageManager = StorageManager.getInstance();
    private LockManager lockManager = LockManager.getInstance();
    private QueryManager queryManager = new QueryManager();

    public CommandResult executeCommand(String command, BufferedWriter writer) throws Exception {
        if(command.equals("begin")) {
            beginTransaction();
        } else if(command.equals("commit")) {
            commitTransaction(writer);
        }
        CommandResult commandResult = new CommandResult();
        commandResult.setStatus(Consts.STATUS_COMMAND_OK);
        commandResult.setType(Consts.SILENCE);
        return commandResult;
    }

    public void abortTransaction() {
        List<LockInfo> queries = lockQueries.get(Thread.currentThread().getId());

        lockManager.unlockAll();
        lockManager.notifyAll(queries);

        transactionOperation.get(Thread.currentThread().getId()).clear();
        lockQueries.get(Thread.currentThread().getId()).clear();
    }

    public void recoverDB() {
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
        lockQueries.put(Thread.currentThread().getId(), new ArrayList<LockInfo>());
        transactionOperation.put(Thread.currentThread().getId(), new ArrayList<String>());
    }

    public void registerQuery(String query) {
        if(Pattern.matches("select [a-z0-9\\*,]+ from [a-z0-9]+( where .+)*", query)) {
            Matcher m = Pattern.compile("select ([a-z0-9\\*,]+) from ([a-z0-9]+)( where (.+))*").matcher(query);
            if(m.find()) {
                String tableName = m.group(2);

                lockQueries.get(Thread.currentThread().getId()).add(
                        new LockInfo(storageManager.getTable(tableName), Consts.SHARED_LOCK)
                );
            }
        } else if(Pattern.matches("insert into [a-zA-Z0-9]+ values \\(.+\\)", query)) {
            Matcher m = Pattern.compile("insert into ([a-zA-Z0-9]+) values \\((.+)\\)").matcher(query);
            if(m.find()) {
                String tableName = m.group(1);

                lockQueries.get(Thread.currentThread().getId()).add(
                        new LockInfo(storageManager.getTable(tableName), Consts.EXCLUSIVE_LOCK)
                );
            }
        } else if(Pattern.matches("delete from [a-z0-9]+( where .+)*", query)) {
            Matcher m = Pattern.compile("delete from ([a-z0-9]+)( where (.+))*").matcher(query);
            if(m.find()) {
                String tableName = m.group(1);

                lockQueries.get(Thread.currentThread().getId()).add(
                        new LockInfo(storageManager.getTable(tableName), Consts.EXCLUSIVE_LOCK)
                );
            }
        }
        transactionOperation.get(Thread.currentThread().getId()).add(query);
    }

    public void commitTransaction(BufferedWriter wr) throws Exception{
        List<LockInfo> queries = lockQueries.get(Thread.currentThread().getId());
        lockManager.takeLocks(queries);

        runTransation(wr);

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

        lockManager.unlockAll();
        lockManager.notifyAll(queries);

        transactionOperation.get(Thread.currentThread().getId()).clear();
        lockQueries.get(Thread.currentThread().getId()).clear();
    }

    public void runTransation(BufferedWriter writer) throws Exception {
        CommandResult commandResult;
        for(String userInput : transactionOperation.get(Thread.currentThread().getId())){
            commandResult = queryManager.executeCommand(userInput);
            if (commandResult.getStatus() == Consts.STATUS_COMMAND_OK) {
                writer.write(commandResult.toConsoleString());
            } else {
                String userResponse = Consts.MESSAGE_WARNING_INVALID_QUERY + userInput;
                writer.write(userResponse);
                System.out.println("Sent to client" + Thread.currentThread().getName() + ": " + userResponse);
            }
        }
    }

    public void insertValues(PagePointer pointer, Table table) {
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
