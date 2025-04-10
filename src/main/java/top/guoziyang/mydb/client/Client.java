package top.guoziyang.mydb.client;

import top.guoziyang.mydb.transport.Package;
import top.guoziyang.mydb.transport.Packager;

public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        //将输入转化成的字节数组，打包成Package
        Package pkg = new Package(stat, null);
        //发送到
        Package resPkg = rt.roundTrip(pkg);
        if(resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
