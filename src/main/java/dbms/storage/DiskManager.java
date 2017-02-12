package dbms.storage;

import dbms.Consts;
import dbms.schema.Schema;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DiskManager {
    private Pattern pattern = Pattern.compile("(.+):([0-9]+)");

    public static final DiskManager instance = new DiskManager();

    public static DiskManager getInstance() {
        return instance;
    }

    public Page getPage(String pageId) {
        Matcher m = pattern.matcher(pageId);
        if(m.find()) {
            try{
                String filePath = m.group(1);
                Integer pageIndex = Integer.parseInt(m.group(2));

                RandomAccessFile file = new RandomAccessFile(filePath, "rw");

                Integer blocks = (int)(file.length() / (long) Consts.BLOCK_SIZE);

                if(pageIndex >= blocks) return null;

                Page page = new Page(
                        file.getChannel().map(FileChannel.MapMode.READ_WRITE,
                        pageIndex * Consts.BLOCK_SIZE,
                        Consts.BLOCK_SIZE)
                );
                return page;
            } catch (Exception e) {
                System.out.println("Can't read file");
                return null;
            }
        }
        return null;
    }

    public Integer getBlocksCount(String path) {
        try{
            RandomAccessFile file = new RandomAccessFile(path, "r");
            return (int)(file.length() / (long) Consts.BLOCK_SIZE);
        } catch (Exception e) {
            System.out.println("Can't read file");
            return 0;
        }
    }

    public Page allocatePage(String pageId, Schema schema) {
        Matcher m = pattern.matcher(pageId);
        try {
            if(m.find()) {
                    String filePath = m.group(1);
                    Integer pageIndex = Integer.parseInt(m.group(2));

                    RandomAccessFile file = new RandomAccessFile(filePath, "rw");

                    Page page = Page.createPage(
                            file.getChannel().map(FileChannel.MapMode.READ_WRITE,
                                    pageIndex * Consts.BLOCK_SIZE,
                                    Consts.BLOCK_SIZE),
                            schema
                    );

                    return page;
            }
        } catch (Exception e) {
            System.out.println("Can't allocate block");
            return null;
        }
        return null;
    }
}
