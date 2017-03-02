package dbms.storage;

import dbms.schema.Schema;
import dbms.schema.TableSchema;
import dbms.storage.table.RealTable;

import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.function.Function;

public class BufferManager<T extends Page> {
    Function<MappedByteBuffer, T> creator;

    private Schema schema;

    private HashMap<String, T> bufferTable = new HashMap<String, T>();
    private DiskManager<T> diskManager;

    public BufferManager(Schema schema, Function<MappedByteBuffer, T> creator) {
        this.schema = schema;
        this.creator = creator;
        diskManager = new DiskManager<>(creator);
    }

    private String toPageId(Long offset) { // filePath:3:45
        return schema.getDataFilePath() + ":" + offset.toString();
    }

    public boolean isBuffered(Long offset) {
        return this.bufferTable.containsKey(toPageId(offset));
    }

    public void bufferPage(Long offset, T page) {
        this.bufferTable.put(toPageId(offset), page);
    }

    public T getPage(Long offset) { // use long for offsets
        String pageId = toPageId(offset);
        if(isBuffered(offset)) {
            return bufferTable.get(pageId);
        }
        T page = diskManager.getPage(pageId);
        if(page != null) {
            bufferPage(offset, page);
        }
        return page;
    }

    public T getLastPage() {
        Long blocks = diskManager.getBlocksCount(schema.getDataFilePath());
        if(blocks == 0) {
            return null;
        }
        return getPage(blocks-1);
    }

    public T allocateNewPage() {
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
