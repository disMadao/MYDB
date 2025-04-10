package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;//存储当前数据库中active事务，抽象出的事务，包含那些信息点进去看
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {//修改这里为返回上一个版本！！！，问题是现在根本没有版本链条？！！
                //先获取lastPosition，然后就可以调用日志类的方法lastUid。
                long lastUid = dm.getLastUid(entry.getPosition());
                System.out.println("不可视，现在获取到的lastUid是："+lastUid);
                if(lastUid != -1) {//不合法的返回应该只有-1，之前因为读日志位置和长度写错了，出现了lastuid=0的情况，记录下以防之后再出现。
//                    System.out.println("这是进入了版本链，如果接下来查询出了数据，那就说明版本链正常工作了，否则就是中间断了或者根本不能查询出之前的版本，测试过了可视化没问题。");
                    return read(xid,lastUid);
                }
//                而且为什么一次输入中同一个列的最新数据会变成不可见啊
                //所以这里还需要判断一下，有的不可视是因为它真的是旧的版本，而数据库可以读取最新的版本。难道MVCC的回溯判断不能在这里执行？？！
//                System.out.println("执行到了这里？？？？？？？");//为什么执行到了这里还能返回正常的数据？？？？版本链的回溯不是在这里吗》？？？？？？
                return null;
            }
        } finally {
            entry.release();
        }
    }

    /**
     * 只为MVCC准备
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public long getEntryPosition(long xid,long uid) throws Exception  {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
            return entry.getPosition();
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return 0;
            } else {
                throw e;
            }
        } finally {
            entry.release();
        }
    }

    /**
     * MVCC使用，每次读取版本链的最新版本，最新版本的唯一特征就是max字段未设置。
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public boolean isLastestEntryForRow(long uid) throws Exception {
        Entry entry = null;
        try {
            entry = super.get(uid);
            if(entry.getXmax() == 0){
                //未被设置
                return true;
            }else {
                return false;
            }
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

    }

    /**
     * 插入数据的方法， 返回uid
     *  还负责将数据包装成Entry，会自动获取position，但日志中的LastPosition需要上层传入。
     * @param xid
     * @param data
     * @param lastPosition   这个数据版本在日志中的位置
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data,long lastPosition) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        long position = dm.getPosition();//版本在日志中的位置

        byte[] raw = Entry.wrapEntryRaw(xid,position,data);//需要修改这个方法，添加个location。
        return dm.insert(xid, raw,lastPosition);
    }


    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lt.add(xid, uid);
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;//死锁了，t无法获取uid，设置事务t为终止态，然后向上抛异常
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            entry.setXmax(xid);

            return true;

        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    //转DM层的read方法
    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
    
}
