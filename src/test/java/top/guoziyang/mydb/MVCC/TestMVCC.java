package top.guoziyang.mydb.MVCC;

import org.junit.Test;
import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.server.Executor;
import top.guoziyang.mydb.backend.tbm.TableManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.vm.VersionManager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * @Auther: Mr. Mei
 * @Date: 2025/4/7 10:14
 * @Description:
 */
public class TestMVCC {
    String path = "/tmp/mydb";
    long mem = (1 << 20) * 64;

    byte[] BEGIN = "begin isolation level read committed".getBytes();
    byte[] CREATE_TABLE = "create table students name string,age int32,classid int32 (index name age classid)".getBytes();
    byte[] INSERT1 = "insert into students values xiaohong 18 1".getBytes();
    byte[] INSERT2 = "insert into students values xiaoming 18 2".getBytes();
    byte[] SELECT = "select name,age from students where age = 18".getBytes();
    byte[] UPDATE = "update students set name = \"zhangsan\" where age = 18".getBytes();

    private Executor testCreate() throws Exception {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, mem, tm);
        VersionManager vm = VersionManager.newVersionManager(tm, dm);
        TableManager tbm = TableManager.create(path, vm, dm);
        Executor exe = new Executor(tbm);
        exe.execute(CREATE_TABLE);

        return exe;
    }
//    private Executor getTestCreate() throws Exception {
//        TransactionManager tm = TransactionManager.open(path);
//        DataManager dm = DataManager.open(path, mem, tm);
//        VersionManager vm = VersionManager.newVersionManager(tm, dm);
//        TableManager tbm = TableManager.open(path, vm, dm);
//        Executor exe = new Executor(tbm);
////        exe.execute(CREATE_TABLE);
//        return exe;
//    }
    @Test
    public void Test1() throws Exception {
        deleteMYDBFile();
        Executor executor = testCreate();
        executor.execute(INSERT1);
        executor.execute(INSERT2);
        //事务1 开始
        executor.execute(BEGIN);
//        Executor executor1 = getTestCreate();
        //事务2 开始
//        executor1.execute(BEGIN);
        //事务1 更新后查找
//        executor.execute(UPDATE);
        executor.execute(SELECT);
        //事务2 在事务1更新后查找看，应该能查找出旧版本
//        executor1.execute(SELECT);
//        executor.close();
//        deleteMYDBFile();//为什么我放在最后删除不奏效？？？
//        因为excotor占用了其中的文件，就算调用close方法关闭SQL执行器也无法释放那些文件，因为是其内部的DM等占用的文件，而只终止执行器没用。
    }

    @Test
    public void deleteMYDBFile() throws Exception {
        Path startDir = Paths.get("D:\\tmp\\");
        Files.walkFileTree(startDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {



                //mydb.bt, mydb.db, mydb.log, mydb.xid
                boolean flag = file.getFileName().toString().equalsIgnoreCase("mydb.bt");
                flag |= file.getFileName().toString().equalsIgnoreCase("mydb.db");
                flag |= file.getFileName().toString().equalsIgnoreCase("mydb.log");
                flag |= file.getFileName().toString().equalsIgnoreCase("mydb.xid");
                if (flag) {
                    try {
                        Files.delete(file);
                        System.out.println("删除成功: " + file);
                    } catch (IOException e) {
                        System.err.println("删除失败: " + file+" Excep: "+e);

                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
