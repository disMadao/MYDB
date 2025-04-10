package top.guoziyang.mydb.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.parser.statement.Create;
import top.guoziyang.mydb.backend.parser.statement.Delete;
import top.guoziyang.mydb.backend.parser.statement.Insert;
import top.guoziyang.mydb.backend.parser.statement.Select;
import top.guoziyang.mydb.backend.parser.statement.Update;
import top.guoziyang.mydb.backend.parser.statement.Where;
import top.guoziyang.mydb.backend.tbm.Field.ParseValueRes;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.ParseStringRes;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    TableManager tbm;
    long uid;
    String name;
    byte status;
    long nextUid;
    List<Field> fields = new ArrayList<>();

    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for(int j = 0; j < create.index.length; j ++) {
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid);
    }

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;

        while(position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw),0);//插入一张表，这里不存在MVCC版本链，直接置一个0，之所以不用多态是为了防止我有没修改到的地方。
        return this;
    }

    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    /**
     * 更新表中数据，更新了添加了MVCC内容，其中重点就是Position和LastPosition字段。
     *  Position可以直接从日志中读出来，就是这条数据待插入日志中的位置
     *  LastPosition 通过VM模块中的给定方法可能读出来。交给VM插入新版本的方法即可。
     * @param xid
     * @param update
     * @return
     * @throws Exception
     */
    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        //后面用作b+树里面的key
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) { //更新所有匹配的行
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            //更新即先’删除‘再插入，涉及版本链，不是真的删除。还要判断可视化和死锁
            ((TableManagerImpl)tbm).vm.delete(xid, uid);//delete中会设置entry的max，会记更新日志，但这个里面怎么设置lastPosition???，按理来说应该是插入的时候就有了的，这里应该不用动。
            long lastPosition = ((TableManagerImpl)tbm).vm.getEntryPosition(xid,uid);
            System.out.println("更新操作过程中，插入的lastPosition："+lastPosition);
            //用map做个中间形式修改raw为新值
            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            raw = entry2Raw(entry);
            //将新值交给vm插入并得到其uid
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw,lastPosition);//只有这里有MVCC中实际有用的LastPosition。
            //统计更新行数
            count ++;

            for (Field field : fields) {
                if(field.isIndexed()) {//更新b+树
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    public String read(long xid, Select read) throws Exception {

        List<Long> uids = parseWhere(read.where);//这里会把这个列的所有版本都找出来，然后交给vm判断可视化读取
        // 对uids再过滤一次，只保留列的最新的那个版本，保证只有一个。
        StringBuilder sb = new StringBuilder();
        List<Long> rowLastestEntry = new ArrayList<>();
        System.out.println("你所查的符合where的记录数一共有："+uids.size());
        for(long uid : uids) {
            if(((TableManagerImpl)tbm).vm.isLastestEntryForRow(uid)) {
                rowLastestEntry.add(uid);
            }
        }
        System.out.println("你所查的符合where的最新的记录数一共有："+rowLastestEntry.size());
        for (Long uid : rowLastestEntry) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }


    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw,0);//第一条插入的，直接赋值lastPosition为0
        for (Field field : fields) {
            if(field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if(values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }
    public List<Long> findAllForSelectPlus() throws Exception {
        return parseWhere(null);
    }

    //通过where中的字段的索引b+树查找出符合条件的uid
    private List<Long> parseWhere(Where where) throws Exception {
        long l0=0, r0=0, l1=0, r1=0;
        boolean single = false;//or是false，其他是true
        Field fd = null;
        if(where == null) {//没有where条件就用第一个索引
            for (Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = Long.MIN_VALUE;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            for (Field field : fields) {//找到where中的字段
                if(field.fieldName.equals(where.singleExp1.field)) {
                    if(!field.isIndexed()) {//没有索引就报错，只能用有索引的字段做where条件
//                        System.out.println("进入了这里");

                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if(fd == null) {//where中的字段不存在
                throw Error.FieldNotFoundException;
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0; r0 = res.r0;
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        //在b+树中找出满足范围的所有uid
        List<Long> uids = fd.search(l0, r0);
        if(!single) {//or 没法在calWhere方法中合并，只能另外合并
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }
//    //走全局索引
//    private List<Long>  parseWhereForFieldNotIndexed(Where where) throws Exception {
//        Field fd = null;
//        //找出有索引的列，用它找出全表数据
//        for (Field field : fields) {
//            if(field.isIndexed()) {
//                fd = field;
//                break;
//            }
//        }
//        long l0 = Long.MIN_VALUE;
//        long r0 = Long.MAX_VALUE;
//        List<Long> uids = fd.search(l0, r0);
//
//
//    }
    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }


    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                if(res.l1 > res.l0) res.l0 = res.l1;
                if(res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size()-1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
