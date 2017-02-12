package dbms.query.Operations;

import java.util.List;

/**
 * Created by alex on 12.02.17.
 */
public class InsertOperation implements Operation {
    private String entityName;
    private List<String> values;

    public InsertOperation(String name, List<String> v) {
        entityName = name;
        values = v;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
}
