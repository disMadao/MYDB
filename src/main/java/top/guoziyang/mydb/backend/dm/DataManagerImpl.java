package top.guoziyang.mydb.backend.dm;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.dataItem.DataItemImpl;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageOne;
import top.guoziyang.mydb.backend.dm.page.PageX;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.dm.pageIndex.PageIndex;
import top.guoziyang.mydb.backend.dm.pageIndex.PageInfo;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Types;
import top.guoziyang.mydb.common.Error;

/*
*   insert方法不太懂
* */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        //数据无效则释放缓存返回null
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    /**
     *
     * @return 返回待插入数据在日志中的位置，提供给VM
     * @throws Exception
     */
    @Override
    public long getPosition() throws Exception {
        return logger.getPosition();
    }

    @Override
    public long getLastUid(long position) throws Exception {
        return logger.getLastUid(position);
    }


    @Override
    public long insert(long xid, byte[] data,long lastPosition) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);

            byte[] log = Recover.insertLog(lastPosition,xid, pg, raw);//这是第一个插入，所以LastPosition直接置为不合法即可
            logger.log(log);//insert方法中没有上一个版本的日志。所以直接传入0

            short offset = PageX.insert(pg, raw);
            //释放锁，被引用数减一，无人引用（且脏了）时才会写入文件持久化
            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {

                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    /**
     * 为xid生成update日志，仅仅被用于DataItem中的after方法中。这个方法不
     * @param xid
     * @param di
     */
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        //这里从di中解析出来lastPosition再传一次。

        logger.log(log);//这里的更新是dataitem.after中的，这里需要添加lastPosition吗？？？？？
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    //和PageImpl里的同名，这就是继承同一父类的不同子类可以实现同名不同方法，这是多态的
    // 但这个是先走PageImpl里的缓存，没有通过PageCache走页的缓存，没有再在PageCache里走数据库读页。
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
    
}
