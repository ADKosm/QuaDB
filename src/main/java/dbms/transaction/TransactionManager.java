package dbms.transaction;


public class TransactionManager {
    public static final TransactionManager instance = new TransactionManager();

    public TransactionManager getInstance() {
        return instance;
    }
}
