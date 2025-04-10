package top.guoziyang.mydb.backend.parser.statement;

import java.util.Arrays;

/**
 * @Auther: Mr. Mei
 * @Date: 2025/1/22 17:49
 * @Description:
 */
public class SelectPlus {
    public String tableName1;
//    public String[] fields1;
    public String[] fields;//查询的字段
    public String tableName2;

    @Override
    public String toString() {
        return "SelectPlus{" +
                "tableName1='" + tableName1 + '\'' +
                ", fields=" + Arrays.toString(fields) +
                ", tableName2='" + tableName2 + '\'' +
                ", wherePlus=" + wherePlus +
                ", connectionType='" + connectionType + '\'' +
                ", on1='" + on1 + '\'' +
                ", on2='" + on2 + '\'' +
                '}';
    }

    //    public String[] fields2;
    public WherePlus wherePlus;

    public String connectionType;
    public String on1;//on条件
    public String on2;


}
