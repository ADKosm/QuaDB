package dbms.storage;

public class DiskManager {
    public static final DiskManager instance = new DiskManager();

    public static DiskManager getInstance() {
        return instance;
    }

    public Page getPage(String pageId) {
        Page page = new Page();
        return page;
    }
}
