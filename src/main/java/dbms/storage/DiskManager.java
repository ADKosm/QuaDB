package dbms.storage;

import dbms.Consts;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DiskManager<T extends Page> {
    private Pattern pattern = Pattern.compile("(.+):([0-9]+)");

    Function<MappedByteBuffer, T> creator;

    public DiskManager(Function<MappedByteBuffer, T> creator) {
        this.creator = creator;
    }

    public T getPage(String pageId) {
        Matcher m = pattern.matcher(pageId);
        if(m.find()) {
            try{
                String filePath = m.group(1);
                Integer pageIndex = Integer.parseInt(m.group(2));

                RandomAccessFile file = new RandomAccessFile(filePath, "rw");

                Integer blocks = (int)(file.length() / (long) Consts.BLOCK_SIZE);

                if(pageIndex >= blocks) return null;

                T page = creator.apply(
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

    public Long getBlocksCount(String path) {
        try{
            RandomAccessFile file = new RandomAccessFile(path, "r");
            return (file.length() / (long) Consts.BLOCK_SIZE);
        } catch (Exception e) {
            System.out.println("Can't read file");
            return (long)0;
        }
    }

    public T allocatePage(String pageId) {
        Matcher m = pattern.matcher(pageId);
        try {
            if(m.find()) {
                    String filePath = m.group(1);
                    Integer pageIndex = Integer.parseInt(m.group(2));

                    RandomAccessFile file = new RandomAccessFile(filePath, "rw");

                    T page = creator.apply(
                            file.getChannel().map(FileChannel.MapMode.READ_WRITE,
                                    pageIndex * Consts.BLOCK_SIZE,
                                    Consts.BLOCK_SIZE)
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
