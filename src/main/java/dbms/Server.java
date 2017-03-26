package dbms;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dbms.schema.Schema;
import dbms.schema.SchemaManager;
import dbms.transaction.TransactionManager;
import dbms.query.QueryManager;


/**
 * Manage a DBMS server. Entry point.
 */

class Handler implements Runnable {
    private final Socket socket;
    private TransactionManager transactionManager = TransactionManager.getInstance();
    Handler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try{
            RequestHandler worker = new RequestHandler(socket);
            worker.handleRequest();
            socket.close();
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port X or listening for a connection");
        }
    }
}

public class Server {

    private Integer portNumber;

    private TransactionManager transactionManager = TransactionManager.getInstance();
    private SchemaManager schemaManager = SchemaManager.getInstance();
    private final ExecutorService pool = Executors.newCachedThreadPool();


    public Server(Integer portNumber){
        this.portNumber = portNumber;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(portNumber);) {
            System.out.println("dbms.Server started. Listening on Port " +  portNumber);
            transactionManager.recoverDB();
            for(;;) {
                pool.execute(new Handler(serverSocket.accept()));
            }
        } catch (IOException e) {
            pool.shutdown();
        }
    }

    public TransactionManager getTransactionManager() {
        return new TransactionManager();
    }

    public QueryManager getQueryManager() {
        return new QueryManager();
    }

}
