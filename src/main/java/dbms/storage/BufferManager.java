package dbms.storage;

import dbms.query.Computator;
import dbms.query.Operations.FullScanOperation;
import dbms.query.Operations.InsertOperation;
import dbms.query.Operations.Operation;
import dbms.query.QueryPlan;
import dbms.query.QueryResult;
import dbms.schema.Schema;

import java.util.HashMap;

public class BufferManager {
    public static final BufferManager instance = new BufferManager();
    private HashMap<String, Page> bufferTable = new HashMap<String, Page>();
    private DiskManager diskManager = DiskManager.getInstance();

    public static BufferManager getInstance() {
        return instance;
    }

    public QueryResult executeQuery(QueryPlan queryPlan) {
        Computator computator = new Computator();
        for(Operation op : queryPlan.getOperations()) {
            computator.compute(op);
        }
        return computator.getResult();
    }

    public boolean isBuffered(String pageId) {
        return this.bufferTable.containsKey(pageId);
    }

    public void bufferPage(String pageId) {
        // TODO load string
        this.bufferTable.put(pageId, diskManager.getPage(pageId));
    }

    public void bufferPage(String pageId, Page page) {
        this.bufferTable.put(pageId, page);
    }

    public Page getPage(String path, Integer offset) {
        String pageId = path + ":" + offset.toString(); // filePath:3:45
        if(isBuffered(pageId)) {
            return bufferTable.get(pageId);
        }
        Page page = diskManager.getPage(pageId);
        if(page != null) {
            bufferPage(pageId, page);
        }
        return page;
    }

    public Page getLastPage(String path) {
        Integer blocks = diskManager.getBlocksCount(path);
        if(blocks == 0) {
            return null;
        }
        return getPage(path, blocks-1);
    }

    public Page allocateNewPage(Schema schema) {
        String path = schema.getDataFilePath();
        Integer blocks = diskManager.getBlocksCount(path);
        String pageId = path + ":" + blocks.toString();
        return diskManager.allocatePage(pageId, schema);
    }
}
