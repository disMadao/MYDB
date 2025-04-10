package top.guoziyang.mydb.backend.tbm;

import top.guoziyang.mydb.backend.parser.statement.Select;
import top.guoziyang.mydb.backend.parser.statement.SelectPlus;
import top.guoziyang.mydb.backend.parser.statement.SingleExpression;
import top.guoziyang.mydb.backend.parser.statement.WherePlus;
import top.guoziyang.mydb.backend.utils.ParseStringRes;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @Auther: Mr. Mei
 * @Date: 2025/1/22 22:05
 * @Description: 临时生成的中间表，用于两个表的连接操作。流程是，先利用b+树读出两个表存入Table对象，然后for两个表的uid列表，用on字段筛选出符合条件的，然后用where条件再筛一遍。
 *                不懂为什么不直接在判断on的时候判断
 */
public class TableTmp {
    Table table1;
    Table table2;
    TableManager tbm;//需要里面的vm.read方法，其实Table对象里面就有，没啥必要
    SelectPlus selectPlus;
    byte[][] cartersian;//笛卡尔积
    int all_counts = 0;//笛卡尔积的行数
    ArrayList<String> field_types = new ArrayList<>();//各个字段的类型，准备好对应的读取
    ArrayList<String> field_name =new ArrayList<>();//笛卡尔积的表的字段名,做成了student.id形式
   // ArrayList<Integer> field_position;//各个字段的位置,string字段长度不确定，无法提前得到，并不是真的二维表的形式存在的




    public TableTmp(Table table1,Table table2,SelectPlus selectPlus,long xid) {
        this.tbm = table1.tbm;
        this.table1 = table1;
        this.table2 = table2;
        this.selectPlus = selectPlus;
    }

    public void Cartesian(long xid) throws Exception{
        //先笛卡尔积
        List<Long> uids1 = table1.findAllForSelectPlus();
        List<Long> uids2 = table2.findAllForSelectPlus();

        cartersian = new byte[uids1.size() * uids2.size()][];//初始化成一个不等长的二维数组
        Field fd1 = null,fd2 = null;
        for(Field field:table1.fields) {
            if(field.isIndexed()) {
                fd1 = field;
                break;
            }
        }
        for(Field field:table2.fields) {
            if(field.isIndexed()) {
                fd2 = field;
                break;
            }
        }
        if(fd1 == null || fd2 == null) {
            throw Error.FieldNotFoundException;
        }

        for (int i = 0; i < table1.fields.size(); i++) {
            Field tmp = table1.fields.get(i);
            field_types.add(tmp.fieldType);
            field_name.add(table1.name+"."+tmp.fieldName);
        }
        for (int i = 0; i < table2.fields.size(); i++) {
            Field tmp = table2.fields.get(i);
            field_types.add(tmp.fieldType);
            field_name.add(table2.name+"."+tmp.fieldName);
        }
        int counts = 0;
        for (int i = 0; i < uids1.size(); i++) {
            for (int j = 0; j < uids2.size(); j++) {
                byte[] raw1 = ((TableManagerImpl)tbm).vm.read(xid, uids1.get(i));
                byte[] raw2 = ((TableManagerImpl)tbm).vm.read(xid, uids2.get(j));
                if(raw1 == null || raw2 == null) continue;
                cartersian[counts] = new byte[raw1.length+raw2.length];
                System.arraycopy(raw1,0,cartersian[counts],0,raw1.length);
                System.arraycopy(raw2,0,cartersian[counts], raw1.length,raw2.length);//笛卡尔连接成byte数组形式
                ++counts;
            }
        }
        all_counts = counts;
    }
    public void onFilter() throws Exception {
        int counts = 0;
//        System.out.println("on过滤之前的all_counts = "+ all_counts);
        if(selectPlus.connectionType.equals("inner")) {
            for (int i = 0; i < all_counts; i++) {
                String filed1 = selectPlus.on1;
                String filed2 = selectPlus.on2;
//                System.out.println("这一行数据的两个部分的字段名分别是："+filed1+" "+filed2);
                byte[] tg1,tg2;
                tg1 = getTarget(filed1,cartersian[i]);
                tg2 = getTarget(filed2,cartersian[i]);
               // System.out.println("这一行数据的两个字段得到的字节数组数据是 ： "+Arrays.toString(tg1)+" "+Arrays.toString(tg2)+" 判断相等："+Arrays.equals(tg1,tg2));
                if(Arrays.equals(tg1,tg2)) {
                    cartersian[counts] = cartersian[i];
                    counts++;
                }
            }
        }
        all_counts = counts;
//        System.out.println("on过滤之后的all_counts = "+all_counts);
    }
    public void whereFilter() throws Exception {
//        System.out.println("进入where过滤器");
        int counts = 0;//如果从0开始，跳出后记得是all_counts = counts -1
        WherePlus wherePlus = selectPlus.wherePlus;
        if(wherePlus == null) return;
//        System.out.println("这是之前的 all_counts = "+all_counts);
        for (int i = 0; i < all_counts; i++) {
            SingleExpression singleExp1 = wherePlus.singleExp1;
            SingleExpression singleExp2 = wherePlus.singleExp2;

            byte[] tg1,tg2;
            if(singleExp1 != null) {
                tg1 = getTarget(singleExp1.field,cartersian[i]);
                //System.out.println("查到这一行数据中，第一个比较字段的值 = "+Arrays.toString(tg1));
                if(!wherePlusFlag(tg1,singleExp1)) {
//                    System.out.println("终于有个不匹配的");
                    continue;
                }
            }
           if(singleExp2 != null) {
               tg2 = getTarget(singleExp2.field,cartersian[i]);
               if(!wherePlusFlag(tg2,singleExp2)) {
//                   System.out.println("终于有个不匹配的");
                   continue;
               }
           }

            cartersian[counts] = cartersian[i];
            counts++;
        }
        all_counts = counts;
//        System.out.println("这是之后的 all_counts = "+all_counts);
    }
    private boolean wherePlusFlag(byte[] tg,SingleExpression singleExp) {
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
                System.out.println("这两个数组相等不？");
                System.out.println(Arrays.toString(raw));
                System.out.println(Arrays.toString(tg));
                if (Arrays.equals(raw,tg)) {

                    System.out.println("aaaaa "+singleExp.value);

                } else {
                    System.out.println("aaaaa 此处应该有小明："+singleExp.value);
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
    public byte[] getTarget(String filed,byte[] raw) throws Exception{
        int pos = 0,len = 0;
        int i = 0;
        if(filed == null) return null;
        for ( i = 0; i < field_name.size(); i++) {
           // System.out.println("--此次遍历字段："+field_name.get(i));
            if(filed.equals(field_name.get(i))) {
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
            throw Error.FieldNotFoundException;
        }
        if(field_types.get(i).equals("int32") ){
            len = 4;
        }else if(field_types.get(i).equals("int64")) {
            len = 8;
        }else if(field_types.get(i).equals("string")) {
            ParseStringRes res = Parser.parseString(raw);
            len = res.next;
        }
        return Arrays.copyOfRange(raw,pos,pos+len);
    }
    public String print() throws Exception {
        //打印笛卡尔积表
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < all_counts; i++) {
            sb.append(parseEntryTmp(cartersian[i])).append("\n");
        }
        return sb.toString();
    }
    //这个方法原来是用来将列值的byte数组转换成String的中间方法，因为我加了两表连接，自连接的时候有重复列，会覆盖，所以删了，直接将byte数组转化成String了。
//    private String printEntryTmp(Map<String,Object> entry) {
//        StringBuilder sb = new StringBuilder("[");
//        for (int i = 0; i < field_name.size(); i++) {
//            String field = field_name.get(i);
//            sb.append(printValue(entry.get(field),field_types.get(i)));
//            if(i == field_name.size()-1) {
//                sb.append("]");
//            } else {
//                sb.append(", ");
//            }
//        }
//        return sb.toString();
//    }
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

    private String parseEntryTmp(byte[] raw) {
        //这个raw实际是两个raw直接合并的
        StringBuilder sb = new StringBuilder("[");
        int pos = 0;
        for (int i = 0;i < field_types.size();i++) {
            String f_type = field_types.get(i);
            String f_name = field_name.get(i);
            ParseValueResTmp r = parserValue(Arrays.copyOfRange(raw, pos, raw.length),f_type,pos);
            sb.append(printValue(r.v,f_type)) ;
            if(i == field_types.size()-1 ) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
//            entry.put(f_name, r.v);
            pos += r.shift;
        }
        return sb.toString();
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
    public ParseValueResTmp parserValue(byte[] raw,String fieldType,int pos) {
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
