package dbms.transaction;

import dbms.storage.table.Table;

/**
 * Created by alex on 26.03.17.
 */
public class LockInfo {
    private Table table;
    private Integer type;

    public LockInfo(Table table, Integer type) {
        this.table = table;
        this.type = type;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }
}
