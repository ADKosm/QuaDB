package dbms.storage;

import dbms.query.QueryPlan;
import dbms.query.QueryResult;

import java.util.HashMap;

public class BufferManager {
    public static final BufferManager instance = new BufferManager();
    private HashMap<String, Page> bufferTable = new HashMap<String, Page>();
    private DiskManager diskManager = DiskManager.getInstance();

    public BufferManager getInstance() {
        return instance;
    }

    public QueryResult executeQuery(QueryPlan queryPlan) {
        QueryResult queryResult = new QueryResult();
        //TODO write inteaction with DiskManager and inner translation table
        return queryResult;
    }

    public boolean isBuffered(String pageId) {
        return this.bufferTable.containsKey(pageId);
    }

    public void bufferPage(String pageId) {
        // TODO load string
        this.bufferTable.put(pageId, diskManager.getPage(pageId));
    }
}
