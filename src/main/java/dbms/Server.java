package dbms;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import dbms.transaction.TransactionManager;
import dbms.query.QueryManager;


/**
 * Manage a DBMS server. Entry point.
 */
public class Server {

    private Integer portNumber;

    private TransactionManager transactionManager = null;


    public Server(Integer portNumber){
        this.portNumber = portNumber;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(portNumber);) {
            System.out.println("dbms.Server started. Listening on Port " +  portNumber);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                RequestHandler worker = new RequestHandler(clientSocket);
                worker.handleRequest();
                clientSocket.close();
            }
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port "
                    + portNumber + " or listening for a connection");
        }
    }

    public TransactionManager getTransactionManager() {
        return new TransactionManager();
    }

    public QueryManager getQueryManager() {
        return new QueryManager();
    }

}
