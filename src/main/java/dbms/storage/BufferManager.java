package dbms.storage;

import dbms.schema.Schema;
import dbms.schema.TableSchema;
import dbms.storage.table.RealTable;

import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.function.Function;

public class BufferManager {
    private Schema schema;

    private HashMap<String, Page> bufferTable = new HashMap<String, Page>();
    private DiskManager diskManager = new DiskManager();

    public BufferManager(Schema schema) {
        this.schema = schema;
    }

    private String toPageId(Long offset) { // filePath:3:45
        return schema.getDataFilePath() + ":" + offset.toString();
    }

    public boolean isBuffered(Long offset) {
        return this.bufferTable.containsKey(toPageId(offset));
    }

    public void bufferPage(Long offset, Page page) {
        this.bufferTable.put(toPageId(offset), page);
    }

    public Page getPage(Long offset) { // use long for offsets
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
        Long blocks = diskManager.getBlocksCount(schema.getDataFilePath());
        if(blocks == 0) {
            return null;
        }
        return getPage(blocks-1);
    }

    public Page allocateNewPage() {
        Schema schema = this.schema;
        String path = schema.getDataFilePath();
        Long blocks = diskManager.getBlocksCount(path);
        String pageId = path + ":" + blocks.toString();
        return diskManager.allocatePage(pageId);
    }

    public Long getPageCount() {
        return diskManager.getBlocksCount(schema.getDataFilePath());
    }
}
