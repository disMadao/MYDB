package top.guoziyang.mydb.backend.parser.statement;

import java.util.Arrays;

//添加两表链接查询，left join ,right join, full join,union，limit
public class Select {
    public String tableName;
    public String[] fields;
    public Where where;

    @Override
    public String toString() {
        return "Select{" +
                "tableName='" + tableName + '\'' +
                ", fields=" + Arrays.toString(fields) +
                ", where=" + where +
                '}';
    }
}
