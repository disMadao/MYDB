package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;

public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
//    long insert(long xid,byte[] data) throws Exception;//原本的接口
    long insert(long xid, byte[] data,long lastPosition) throws Exception;//MVCC使用
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    long getEntryPosition(long xid,long uid) throws Exception;//MVCC需要
    boolean isLastestEntryForRow(long uid) throws Exception;//MVCC使用，让系统每次读的都是行的最新版本，然后不可视才依据版本链回溯。原系统是读所有版本然后一一判断可视化。

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
