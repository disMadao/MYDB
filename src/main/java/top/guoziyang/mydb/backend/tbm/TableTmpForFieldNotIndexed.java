package top.guoziyang.mydb.backend.tbm;

import top.guoziyang.mydb.backend.parser.statement.Select;
import top.guoziyang.mydb.backend.parser.statement.SelectPlus;
import top.guoziyang.mydb.backend.parser.statement.SingleExpression;
import top.guoziyang.mydb.backend.utils.ParseStringRes;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @Auther: Mr. Mei
 * @Date: 2025/2/6 00:27
 * @Description: 当普通select的where字段没有索引的时候，就使用这个类，使用主键索引查找出数据然后过滤，这一步就和TableTmp中一样了。
 *
 */
public class TableTmpForFieldNotIndexed {

    Table table;
    Select select;//这是为了走全局索引的而设立的方法。
    TableManager tbm;//需要里面的vm.read方法，其实Table对象里面就有，没啥必要

    byte[][] data;//
    int all_counts = 0;//数据的行数
    ArrayList<String> field_types = new ArrayList<>();//各个字段的类型，准备好对应的读取
    ArrayList<String> field_name =new ArrayList<>();//笛卡尔积的表的字段名,做成了student.id形式


    public TableTmpForFieldNotIndexed(Table table,Select select,long xid) {
        this.table = table;
        this.tbm = table.tbm;
        this.select = select;
//        this.xid = xid;
    }
    //这个方法就不向上抛异常了，直接打印出
    public void setDataForFieldNotIndexed(long xid) {
        try{
            List<Long> uids = table.findAllForSelectPlus();
            //然后读取byte数组构成数据，然后开始where过滤
            data = new byte[uids.size()][];
            Field fd1 = null;
            for(Field field:table.fields) {
                if(field.isIndexed()) {
                    fd1 = field;
                    break;
                }
            }
            if(fd1 == null) {
                throw Error.FieldNotFoundException;
            }
            for (int i = 0; i < table.fields.size(); i++) {
                Field tmp = table.fields.get(i);
                field_types.add(tmp.fieldType);
                field_name.add(tmp.fieldName);
            }
            int counts = 0;
//            System.out.println(uids.toString());
            SingleExpression singleExpression = select.where.singleExp1;
            for(int i = 0; i < uids.size(); i ++) {
                byte[] raw = ((TableManagerImpl)tbm).vm.read(xid,uids.get(i));
                if(raw == null) continue;
//                data[i] = raw;
                //从这行数据中获取需要比较的字段的值。
                byte[] tg = getTarget(singleExpression.field,raw);
                if(!whereFlag(tg,singleExpression)) {
                    //不匹配，跳出此次
//                    System.out.println("跳过一次数据！");
                    continue;
                }
                data[counts] = raw;
                counts++;
            }
            all_counts = counts;
        } catch (Exception e) {
//            System.out.println("出错了，TableTempForFieldNotIndexed里面");
            e.printStackTrace();
        }

    }
    //从raw这行数据中获取 field列的数据
    private byte[] getTarget(String field,byte[] raw) throws Exception {
        int pos = 0,len = 0;
        int i = 0;
        if(field == null) return null;
//        System.out.println("当前寻找的字段："+filed);
//        System.out.println("当前临时表里的总字段 = "+field_name.toString());
        for ( i = 0; i < field_name.size(); i++) {
            // System.out.println("--此次遍历字段："+field_name.get(i));
            if(field.equals(field_name.get(i))) {
                break;
            }
            if(field_types.get(i).equals("int32") ){
                len = 4;
                pos += 4;
            }else if(field_types.get(i).equals("int64")) {
                len = 8;
                pos += 8;
            }else if(field_types.get(i).equals("string")) {
                //注意这里不能传送raw，因为parseString方法从0字节开始
                ParseStringRes res = Parser.parseString(Arrays.copyOfRange(raw,pos,raw.length));
                len = res.next;
                pos += res.next;
            }
        }
        if(i == field_name.size()) {
//            System.out.println(field);
//            System.out.println(field_name.toString());
            throw Error.FieldNotFoundException;
        }
        if(field_types.get(i).equals("int32") ){
//            System.out.println(filed+" 是int32");
            len = 4;
        }else if(field_types.get(i).equals("int64")) {
//            System.out.println(filed+" 是int64");
            len = 8;
        }else if(field_types.get(i).equals("string")) {
//            System.out.println(filed+" 是string");
            ParseStringRes res = Parser.parseString(raw);
            len = res.next;
        }
//        System.out.println();
//        System.out.println("此次查找的字段名是 ： "+field_name.get(i));
//        System.out.println("查找到了，原数组 = "+ Arrays.toString(raw)+"，位置 = "+pos+", 查找结果："+Arrays.toString(Arrays.copyOfRange(raw,pos,pos+len)));
        //  System.out.println("pos = "+pos+",len = "+len+"，raw.len = "+raw.length);
        return Arrays.copyOfRange(raw,pos,pos+len);
    }

    private boolean whereFlag(byte[] tg,SingleExpression singleExp) {
        HashSet<String> longSet = new HashSet<>(Arrays.asList(">", "<"));
        if(longSet.contains(singleExp.compareOp)) {
            //这个字段应该是int型号
            long tmpLong = 0;
            if(tg.length == 4) {
                tmpLong = ((tg[0] & 0xFF) << 24) |
                        ((tg[1] & 0xFF) << 16) |
                        ((tg[2] & 0xFF) << 8)  |
                        (tg[3] & 0xFF);
            }else {
                tmpLong = ((tg[0] & 0xFFL) << 56) |
                        ((tg[1] & 0xFFL) << 48) |
                        ((tg[2] & 0xFFL) << 40) |
                        ((tg[3] & 0xFFL) << 32) |
                        ((tg[4] & 0xFFL) << 24) |
                        ((tg[5] & 0xFFL) << 16) |
                        ((tg[6] & 0xFFL) << 8)  |
                        (tg[7] & 0xFFL);
            }
//            System.out.println("tmpLong = "+tmpLong+", singleExp.value = "+Long.valueOf(singleExp.value)+", "+singleExp.compareOp);
            if(singleExp.compareOp.equals(">") && Long.valueOf(singleExp.value) > tmpLong) {

            }else if(singleExp.compareOp.equals("<") && Long.valueOf(singleExp.value) < tmpLong) {

            }else {

                return false;//跳过这个不合格数据
            }
        }else {//直接按字符串比较
            if(singleExp.compareOp.equals("=")) {

                int tmpNo = getNoFromName(singleExp.field);
                byte[] raw = new byte[1];
                switch(field_types.get(tmpNo)) {
                    case "int32":
                        raw = Parser.int2Byte(Integer.valueOf(singleExp.value));
                        break;
                    case "int64":
                        raw = Parser.long2Byte(Long.valueOf(singleExp.value));
                        break;
                    case "string":
                        raw = Parser.string2Byte(singleExp.value);
                        break;
                }
                if (Arrays.equals(raw,tg)) {
//                    System.out.println("aaaaa "+singleExp.value);

                } else {
//                    System.out.println("aaaaa 此处应该有小明："+singleExp.value);
                    return false;
                }
            }else if(singleExp.compareOp.equals("!=")) {//其实还不支持!=，改动的话，要改之前的太多了，暂时不该吧
                if (singleExp.value.equals(new String(tg, StandardCharsets.UTF_8))) {
                    return false;
                } else {

                }
            }
        }
        return true;
    }
    private int getNoFromName(String name) {
        for (int i = 0; i < field_name.size(); i++) {
            if(field_name.get(i).equals(name)) {
                return i;
            }
        }
        return -1;//出问题了
    }

    public String print() throws Exception {
        //打印笛卡尔积表
        //  System.out.println("正式进入");
        StringBuilder sb = new StringBuilder();
//        System.out.println("我最后得到的数据总行数： "+all_counts);
        for (int i = 0; i < all_counts; i++) {
            Map<String,Object> entry = parseEntryTmp(data[i]);
            //System.out.println("开始打印，，entry = "+entry);
            sb.append(printEntryTmp(entry)).append("\n");
        }
        //先打印表头
//        System.out.println(field_name.toString());
//        System.out.println(sb.toString());
        return sb.toString();
    }
    private String printEntryTmp(Map<String,Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < field_name.size(); i++) {
            String field = field_name.get(i);
            sb.append(printValue(entry.get(field),field_types.get(i)));
            if(i == field_name.size()-1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
    //三种类型都转string
    public String printValue(Object v,String type) {
        String str = null;
        switch(type) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    private Map<String, Object> parseEntryTmp(byte[] raw) {
        //这个raw实际是两个raw直接合并的
        int pos = 0;
        //  System.out.println("打印raw数组看看： "+Arrays.toString(raw));
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0;i < field_types.size();i++) {
            String f_type = field_types.get(i);
            String f_name = field_name.get(i);
            ParseValueResTmp r = parserValue(Arrays.copyOfRange(raw, pos, raw.length),f_type,pos);
            // System.out.println("当前查到的 r ="+r.toString());
            entry.put(f_name, r.v);
            pos += r.shift;
        }
        return entry;
    }
    public class ParseValueResTmp{
        Object v;
        int shift;

        @Override
        public String toString() {
            return "ParseValueResTmp{" +
                    "v=" + v +
                    ", shift=" + shift +
                    '}';
        }
    }
    public ParseValueResTmp parserValue(byte[] raw, String fieldType, int pos) {
        ParseValueResTmp res = new ParseValueResTmp();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

}
