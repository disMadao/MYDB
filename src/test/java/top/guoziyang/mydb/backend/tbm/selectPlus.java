package top.guoziyang.mydb.backend.tbm;

import org.junit.Test;
import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.parser.Parser;
import top.guoziyang.mydb.backend.parser.statement.*;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.vm.VersionManager;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * @Auther: Mr. Mei
 * @Date: 2025/1/23 03:23
 * @Description:
 */
public class selectPlus {

    public static void main(String[] args) throws Exception {
        File f = new File("D:/tmp/test1");
        File[] fs = f.listFiles();
        for(File tmp : fs) {
            tmp.delete();
        }
        TransactionManagerImpl tm= TransactionManager.create("D:/tmp/test1/tm");
        DataManager dm=DataManager.create("D:/tmp/test1/dm",1 << 20,tm);
        VersionManager vm=VersionManager.newVersionManager(tm,dm);
        TableManager tbm=TableManager.create("D:/tmp/test1/",vm,dm);
        //开启事务
        BeginRes br=tbm.begin(new Begin());
        long xid=br.xid;
        //建立一张新表
        String ss="create table students " +
                "name string,age int32,classid int32 " +
                "(index name age classid)";
        byte b[]=ss.getBytes(StandardCharsets.UTF_8);
        Object stat = Parser.Parse(b);
        tbm.create(xid,(Create) stat);
        //建立另一张表
        String ss1="create table class " +
                "name string,id int32 " +
                "(index name id)";
        byte b1[]=ss1.getBytes(StandardCharsets.UTF_8);
        Object stat1 = Parser.Parse(b1);
        tbm.create(xid,(Create) stat1);


        System.out.println("===测试插入操作===");
        ss="insert into students values xiaohong 18 1";
        b=ss.getBytes(StandardCharsets.UTF_8);
        stat = Parser.Parse(b);
        tbm.insert(xid,(Insert) stat);
        ss="insert into students values xiaoming 18 2";
        b=ss.getBytes(StandardCharsets.UTF_8);
        stat = Parser.Parse(b);
        tbm.insert(xid,(Insert) stat);
        //向第二张表插入
        ss="insert into class values yiban 1";
        b=ss.getBytes(StandardCharsets.UTF_8);
        stat = Parser.Parse(b);
        tbm.insert(xid,(Insert) stat);

        System.out.println("===测试查询操作===");
        ss="select name,age from students";
        b=ss.getBytes(StandardCharsets.UTF_8);
        stat = Parser.Parse(b);
        byte[] output=tbm.read(xid,(Select) stat);
        System.out.println(new String(output));
        //试试我的
        System.out.println("进入了我的方法！！！！！");
        //on不能进行自连接，因为将两个同样的行连接的时候，查询查到第一个字段就会返回，那就一定相同
        ss="selectplus * from students inner join students on students.classid = students.classid where students.name = xiaohong";
        b=ss.getBytes(StandardCharsets.UTF_8);
        stat = Parser.Parse(b);
        System.out.println("解析完毕得到 stat = "+stat.toString());//这里没问题！！！！


        output = tbm.readPlus(xid,(SelectPlus) stat);
        System.out.println("开始打印selectplus结果");
        System.out.println(new String(output));
        System.out.println("打印完毕");


        System.out.println("===测试删除操作===");
        ss=" delete from students where age =18";
        b=ss.getBytes(StandardCharsets.UTF_8);
        stat = Parser.Parse(b);
        tbm.delete(xid,(Delete) stat);
        ss=" delete from students where age =17";
        b=ss.getBytes(StandardCharsets.UTF_8);
        stat = Parser.Parse(b);
        tbm.delete(xid,(Delete) stat);
    }
    @Test
    public void test1() throws Exception {
        File f = new File("D:/tmp/test1");
        File[] fs = f.listFiles();
        for(File tmp : fs) {
            tmp.delete();
        }
        TransactionManagerImpl tm= TransactionManager.create("D:/tmp/test1/tm");
        DataManager dm=DataManager.create("D:/tmp/test1/dm",1 << 20,tm);
        VersionManager vm=VersionManager.newVersionManager(tm,dm);
        TableManager tbm=TableManager.create("D:/tmp/test1/",vm,dm);
        //开启事务
        BeginRes br=tbm.begin(new Begin());
        long xid=br.xid;
        //建立一张新表
        String ss="create table students " +
                "name string,age int32,classid int32 " +
                "(index age)";
        byte b[]=ss.getBytes(StandardCharsets.UTF_8);
        Object stat = Parser.Parse(b);
        tbm.create(xid,(Create) stat);
        System.out.println("===测试插入操作===");
        ss="insert into students values xiaohong 18 1";
        b=ss.getBytes(StandardCharsets.UTF_8);
        stat = Parser.Parse(b);
        tbm.insert(xid,(Insert) stat);
        ss="insert into students values xiaoming 17 2";
        b=ss.getBytes(StandardCharsets.UTF_8);
        stat = Parser.Parse(b);
        tbm.insert(xid,(Insert) stat);

        // 1. 输出当前系统时间
        LocalDateTime currentTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSS");

        for(int i = 0; i < 10000; i ++) {

            ss="select * from students where age =18";
            b=ss.getBytes(StandardCharsets.UTF_8);
            stat = top.guoziyang.mydb.backend.parser.Parser.Parse(b);
            byte[] output=tbm.read(xid,(Select) stat);
            System.out.println(new String(output));
        }
        System.out.println("查询开始前的系统时间: " + currentTime.format(formatter));
        LocalDateTime currentTime2 = LocalDateTime.now();
        DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSS");
        System.out.println("查询结束后的系统时间: " + currentTime2.format(formatter2));
        Duration duration = Duration.between(currentTime,currentTime2);
        long totalNanos = duration.toNanos();
        double seconds = totalNanos / 1_000_000_000.0;
        System.out.println("时间差-秒："+seconds);
        System.out.println("时间差-纳秒："+totalNanos);





    }
}
