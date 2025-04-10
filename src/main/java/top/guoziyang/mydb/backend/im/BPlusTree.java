package top.guoziyang.mydb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.im.Node.InsertAndSplitRes;
import top.guoziyang.mydb.backend.im.Node.LeafSearchRangeRes;
import top.guoziyang.mydb.backend.im.Node.SearchNextRes;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Parser;

public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;//比较特殊，整个bootDatItem里面只存了实际根节点的uid，因为可能分裂出新的根节点，所以需要做一层封装
    Lock bootLock;


    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        //将Node形式的字节数组抛给dm插入
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot,Node.NO_LASTPOSITION);
        //将包含Node的DataItem的uid插入dm，这个新的dataItem只保存了Node的‘指针’
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid),Node.NO_LASTPOSITION);
    }

    //替代构造方法
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    //向根节点，插入一个新节点（right,rightKey)，这里left是原来的根节点uid
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            //生成一个新根节点（left,rightKey）|（right，MAX_VALUE）
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            //将这个新根节点抛给dm插入并生成uid
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw,Node.NO_LASTPOSITION);
            bootDataItem.before();
            //获取当前根节点的数据
            SubArray diRaw = bootDataItem.data();
            //修改bootDateItem的uid为新根节点的uid
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    //查找key所在节点是否在nodeUid中的子节点中，在就返回其Uid，否则返回兄弟节点uid
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);//按从左到右的顺序获得node
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }
    //最上层的insert方法
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) { //需要向根节点插入节点
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    //插入之后可能返回分裂，分裂则返回新节点，如果是最初调用的insert，则会返回的分裂节点，插入根节点中
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {//叶子节点，直接插入
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);//查找nodeUid中是否有，没有就返回兄弟节点
            InsertRes ir = insert(next, uid, key);//向next递归插入
            if(ir.newNode != 0) {//递归中分裂出了一个新节点，插入当前节点
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    //返回插入后分裂的新节点，没有分裂产生新节点的话里面的值全0
    //思路：通过nodeUid和当前b+树得到node，然后递归调用node.insertAndSplit
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            //向node插入(uid,key)
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {//返回邻节点，继续下次循环向邻节点插入
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
