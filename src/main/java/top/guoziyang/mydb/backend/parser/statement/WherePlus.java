package top.guoziyang.mydb.backend.parser.statement;

/**
 * @Auther: Mr. Mei
 * @Date: 2025/1/22 17:50
 * @Description:
 */
//相较where，没有改动，读取的时候再解析SingleExpression
public class WherePlus {
    public SingleExpression singleExp1;
    public String logicOp;
    public SingleExpression singleExp2;

    @Override
    public String toString() {
        return "WherePlus{" +
                "singleExp1=" + singleExp1 +
                ", logicOp='" + logicOp + '\'' +
                ", singleExp2=" + singleExp2 +
                '}';
    }
}
