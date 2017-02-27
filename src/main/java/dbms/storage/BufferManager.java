package dbms.storage;

import dbms.query.Computator;
import dbms.query.Operations.FullScanOperation;
import dbms.query.Operations.InsertOperation;
import dbms.query.Operations.Operation;
import dbms.query.QueryPlan;
import dbms.query.QueryResult;
import dbms.schema.Schema;
import dbms.schema.dataTypes.Int;
import dbms.storage.table.RealTable;
import dbms.storage.table.Table;

import java.util.HashMap;

public class BufferManager {
    private RealTable table;

    private HashMap<String, Page> bufferTable = new HashMap<String, Page>();
    private DiskManager diskManager = new DiskManager();

    public BufferManager(RealTable table) {
        this.table = table;
    }

    private String toPageId(Integer offset) { // filePath:3:45
        return table.getSchema().getDataFilePath() + ":" + offset.toString();
    }

    public boolean isBuffered(Integer offset) {
        return this.bufferTable.containsKey(toPageId(offset));
    }

    public void bufferPage(Integer offset, Page page) {
        this.bufferTable.put(toPageId(offset), page);
    }

    public Page getPage(Integer offset) {
        String pageId = toPageId(offset);
        if(isBuffered(offset)) {
            return bufferTable.get(pageId);
        }
        Page page = diskManager.getPage(pageId);
        if(page != null) {
            bufferPage(offset, page);
        }
        return page;
    }

    public Page getLastPage() {
        Integer blocks = diskManager.getBlocksCount(table.getSchema().getDataFilePath());
        if(blocks == 0) {
            return null;
        }
        return getPage(blocks-1);
    }

    public Page allocateNewPage() {
        Schema schema = table.getSchema();
        String path = schema.getDataFilePath();
        Integer blocks = diskManager.getBlocksCount(path);
        String pageId = path + ":" + blocks.toString();
        return diskManager.allocatePage(pageId, schema);
    }
}
