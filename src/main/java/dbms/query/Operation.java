package dbms.query;

public class Operation {
    private String entityName;
    private int type;
    private String predicate;

    public Operation(String entityName, int type, String predicate) {
        this.entityName = entityName;
        this.type = type;
        this.predicate = predicate;
    }

    public Operation(String entityName, int type) {
        this.entityName = entityName;
        this.type = type;
    }
}
