package dbms.transaction;

import dbms.Consts;
import dbms.schema.dataTypes.Pointer;
import dbms.storage.table.Table;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by alex on 26.03.17.
 */
public class LockManager {
    public static final LockManager instance = new LockManager();
    public static LockManager getInstance() {
        return instance;
    }

    private HashMap<Long, Set<Table> > sharedLocks = new HashMap<>();
    private HashMap<Table, Set<Long> > sharedLockedTables = new HashMap<>();

    private HashMap<Long, Set<Table> > exclusiveLocks = new HashMap<>();
    private Set<Table> exclusiveLockedTables = new HashSet<>();

    private ReentrantLock lockToLock = new ReentrantLock();
    private ReentrantLock takeLocksLock = new ReentrantLock();

    private List<LockInfo> buildLocksPlan(List<LockInfo> queries) {
        HashMap<Table, Integer> map = new HashMap<>();
        for(LockInfo info : queries) {
            if(info.getType().equals(Consts.SHARED_LOCK)) {
                if(!map.containsKey(info.getTable())) {
                    map.put(info.getTable(), info.getType());
                }
            } else if(info.getType().equals(Consts.EXCLUSIVE_LOCK)) {
                if(!map.containsKey(info.getTable())) {
                    map.put(info.getTable(), info.getType());
                }
                if(map.containsKey(info.getTable()) && map.get(info.getTable()).equals(Consts.SHARED_LOCK)) {
                    map.replace(info.getTable(), info.getType());
                }
            }
        }

        List<LockInfo> result = new ArrayList<>();
        for(Table table : map.keySet()) {
            result.add(new LockInfo(table, map.get(table)));
        }
        return result;
    }

    public void takeLocks(List<LockInfo> queries){
        List<LockInfo> plan = buildLocksPlan(queries);
        boolean success = false;
        Table lockedTable = null;
        while (!success) {
            takeLocksLock.lock();
            if(lockedTable != null) {
                takeLocksLock.unlock();
                lockedTable.waitForUnlock();
                takeLocksLock.lock();
            }

            for(int i = 0; i < plan.size(); i++) {
                Table table = plan.get(i).getTable();
                Integer type = plan.get(i).getType();
                if(tryLock(table, type)) {
                    success = (i == plan.size()-1);
                } else {
                    lockedTable = table;
                    unlockAll();
                    break;
                }
            }
            takeLocksLock.unlock();
        }
    }

    public void unlockAll(){
        lockToLock.lock();

        if(sharedLocks.containsKey(Thread.currentThread().getId())) {
            for(Table table : sharedLocks.get(Thread.currentThread().getId())) {
                sharedLockedTables.get(table).remove(Thread.currentThread().getId());
            }
            sharedLocks.remove(Thread.currentThread().getId());
        }

        if(exclusiveLocks.containsKey(Thread.currentThread().getId())) {
            for(Table table : exclusiveLocks.get(Thread.currentThread().getId())) {
                exclusiveLockedTables.remove(table);
            }
            exclusiveLocks.remove(Thread.currentThread().getId());
        }

        lockToLock.unlock();
    }

    public void notifyAll(List<LockInfo> queries){
        for (LockInfo info : queries) {
            info.getTable().notifyOne();
        }
    }

    private boolean tryLock(Table table, Integer type) {
        if(type.equals(Consts.EXCLUSIVE_LOCK)) {
            return tryExclusiveLock(table);
        } else if(type.equals(Consts.SHARED_LOCK)) {
            return trySharedLock(table);
        }
        return false;
    }

    private boolean trySharedLock(Table table) {
        lockToLock.lock();

        if(exclusiveLockedTables.contains(table)) {
            lockToLock.unlock();
            return false; // cannot take lock cause of exclusive lock
        }

        if(!sharedLocks.containsKey(Thread.currentThread().getId())) {
            sharedLocks.put(Thread.currentThread().getId(), new HashSet<Table>());
        }
        sharedLocks.get(Thread.currentThread().getId()).add(table);

        if(!sharedLockedTables.containsKey(table)) {
            sharedLockedTables.put(table, new HashSet<Long>());
        }
        sharedLockedTables.get(table).add(Thread.currentThread().getId());

        lockToLock.unlock();
        return true;
    }

    private boolean tryExclusiveLock(Table table) {
        lockToLock.lock();

        if(exclusiveLockedTables.contains(table) || sharedLockedTables.getOrDefault(table, new HashSet<Long>()).size() > 0) {
            lockToLock.unlock();
            return false; // cannot take lock caouse of ex or shared lock
        }

        if(!exclusiveLocks.containsKey(Thread.currentThread().getId())) {
            exclusiveLocks.put(Thread.currentThread().getId(), new HashSet<Table>());
        }
        exclusiveLocks.get(Thread.currentThread().getId()).add(table);

        exclusiveLockedTables.add(table);

        lockToLock.unlock();
        return true;
    }


}
