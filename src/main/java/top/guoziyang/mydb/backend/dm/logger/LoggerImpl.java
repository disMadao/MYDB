package top.guoziyang.mydb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.dm.Recover;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.backend.utils.Types;
import top.guoziyang.mydb.common.Error;

/**
 * 日志文件读写
 * 
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * 
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;//这是为了防止Hash冲突取的质数

//    private static final int OF_LastPOSITION = 0;
    //下次最后把每个字段的长度也包装成一个变量
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;
    
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;  //效验和？？

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }
    /*
    * 从文件中读取开头4字节，初始化checksum值
    * 然后调用checkAndRemoveTail，去除尾部，只保留checksum在文件中
    * */
    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }
        //用raw存储从fc开头读取的4字节数据
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //利用这个raw得到checksum
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        //来到checksum后面
        rewind();

        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 将data内容写入日志文件，并更新checksum，这里还需要传入一个LastPosition字段。
     * @param data
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    /**
     *
     * @return 下一个日志待插入的位置，只是为了提供给VM中设置Entry的Position字段。
     */
    @Override
    public long getPosition() {
        lock.lock();
        try {
            return fc.size();
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        return -1;//不可能到这里来的。
    }

    /**
     * 需要传入position（当前版本所在日志中的位置），然后通过position获取其lastPosition，先定位到LastPosition位置，解析出uid，然后重定位回去，然后再返回uid。
     *
     * 这一段读日志的逻辑和next一样，就是把position换成了lastPosition
     * @return
     */
    @Override
    public long getLastUid(long position) {
        System.out.println("position: "+position);
        long lastPosition = getLastPosition(position);
        System.out.println("lastPosition:"+lastPosition);
        if(lastPosition == 0) return -1;//因为一些问题（详见博客），需要在这里再次判断是否到达了版本链的最初版本。
        //问题的根源！！lastPosition为0，但如果lastPosition为0的话根本就走不动这个方法啊，说明它没有出现版本问题！！
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(lastPosition);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        System.out.println("size："+size);//显出这个值是负值？？！！？？！？！？！？？？！？！？！？！？！？！？！？！？！

        ByteBuffer buf = ByteBuffer.allocate(size);//把[size][checksum][data]都读出来，然后再去解析data
        //data：[lastposition] [LogType] [XID] [Pgno] [Offset] [Raw]
        try {
            fc.position(lastPosition+OF_DATA);
            fc.read(buf);//读取数据部分
        } catch(IOException e) {
            Panic.panic(e);
        }
        byte[] log = buf.array();
//      这里就不检查了，不涉及更改，只是读而已。而且这里的位置被我改的有点乱了。
//        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
//        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));//这里也是写死了的，从of_checksum开始的四个字节数据
//        if(checkSum1 != checkSum2) {
//            Panic.panic(Error.BadLogFileException);//这个检查有必要吗？？
//        }
        int pgno = Recover.getPgno(log);
        short offset = Recover.getOffset(log);
        System.out.println("解析的页面和偏移量分别为："+pgno+","+offset);
        long lastUid = Types.addressToUid(pgno,offset);
        return lastUid;
    }
    //这个方法有问题。有点脱裤子放屁的味道，可以直接读出来的。非要这里引用那里引用。
    private long getLastPosition(long position) {
        ByteBuffer tmp = ByteBuffer.allocate(4);//获取这个日志的size字段

        try {
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());

        ByteBuffer buf = ByteBuffer.allocate(size);//[size][checksum][data]，只读取data部分，交给Recover解析
        //data：[lastposition] [LogType] [XID] [Pgno] [Offset] [Raw]
        try {
            fc.position(position+OF_DATA);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        byte[] log = buf.array();
        return Recover.getLastPosition(log);
//        ByteBuffer tmp = ByteBuffer.allocate(8);//直接解析式出lastPosition
//        try {
//            fc.position(position+OF_DATA);
//            fc.read(tmp);
//        } catch(IOException e) {
//            Panic.panic(e);
//        }
//        return
    }


    /*
    * 不将原checksum重置成0直接利用log数组更新，然后覆盖文件开头的原来的checksum
    * */
    private void updateXChecksum(byte[] log) {
        //这个值居然不用重置成0
        this.xChecksum = calChecksum(this.xChecksum, log);
        //将新的xChecksum写入文件的开始位置，覆盖原来的值，并强制刷新进硬件(force)
        //如果想不覆盖开头，没有简单方法，只能后移原有数据。
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 包装日志，添加一个LastPosition字段。
     * @param data
     * @return
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        //concat方法用于合并多个数组返回一个长数组
        return Bytes.concat(size,checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }
    /*
    * 这个类最主要的方法，有点难懂。
    *这个方法是从文件中固定读取4字节的数据，4字节是因为作者写死了4字节。
    * 作者写死4字节是因为后面有个字节数组转Int，java里面是默认读取4字节转换。
    * 简而言之：4字节是为了转换成int
    * 最后有个效验和的判断比较有点问题，我看不懂作者的意图是什么？？？
    * */
    private byte[] internNext() {
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if(position + size + OF_DATA > fileSize) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        byte[] log = buf.array();

        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));//这里也是写死了的，从of_checksum开始的四个字节数据
        if(checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    /*
    * 调用internNext，得到一个byte数组，复制其内容返回一个新的数组
    * */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    /*
    * 倒带
    * */
    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    
}
