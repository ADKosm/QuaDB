package dbms.query.Operations;

public class FullScanOperation implements Operation {
    private String entityName;
    private String predicate;

    public FullScanOperation(String entityName, String predicate) {
        this.entityName = entityName;
        this.predicate = predicate;
    }

    public FullScanOperation(String entityName) {
        this.entityName = entityName;
    }


    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }
}
