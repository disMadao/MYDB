package top.guoziyang.mydb.backend.parser;

import java.util.ArrayList;
import java.util.List;

import top.guoziyang.mydb.backend.parser.statement.*;
import top.guoziyang.mydb.common.Error;

public class Parser {

    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            switch(token) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "selectplus":
//                    System.out.println("进入第一步");
                    stat = parseSelectPlus(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    throw Error.InvalidCommandException;
            }
        } catch(Exception e) {
            statErr = e;
        }
        try {
            String next = tokenizer.peek();
            if(!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch(Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if(statErr != null) {
            throw statErr;
        }
        return stat;
    }

    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            return new Show();
        }
        throw Error.InvalidCommandException;
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();
        tokenizer.pop();

        if(!"set".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        if(!"=".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if(!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if(!"into".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if(!"values".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> values = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);

        return insert;
    }

    //selectPlus功能
    private static SelectPlus parseSelectPlus(Tokenizer tokenizer) throws Exception {
        SelectPlus read = new SelectPlus();
        List<String> fields = new ArrayList<>();//字段，两表链接的话，应该类似student.id

//        System.out.println("进入paraSelectPlus");
        String asterisk = tokenizer.peek();
        System.out.println(asterisk);
        if("*".equals(asterisk)) {//查询两个表的所有字段
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while (true) {
                String field = tokenizer.peek();
                if (!isName(field)) {
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if (",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
//        System.out.println("查询字段解析完成");
        read.fields = fields.toArray(new String[fields.size()]);
        if(!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        String tableName1 = tokenizer.peek();//第一个表
        if(!isName(tableName1)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        String connectionType = tokenizer.peek();//取出连接方式，现在只支持inner

        tokenizer.pop();
        if(!"join".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        String tableName2 = tokenizer.peek();//第二个表
        if(!isName(tableName2)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        read.tableName1 = tableName1;
        read.tableName2 = tableName2;
        read.connectionType = connectionType;
        if(!"on".equals(tokenizer.peek())) {//on字段，后面跟连接的字段
            throw Error.InvalidCommandException;
        }
//        System.out.println("解析到了on后面");
        tokenizer.pop();
        String on = tokenizer.peek();//on就暂时不判断了。后面再改
        tokenizer.pop();
        read.on1 = on;
//        System.out.println("得到了on1");
        if(!"=".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        read.on2 = tokenizer.peek();
        tokenizer.pop();

//        System.out.println("得到的on1："+read.on1+" on2："+read.on2);
        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
//            System.out.println("解析出当前where条件为空");
            read.wherePlus = null;
            return read;
        }
//        System.out.println("开始解析where条件");

        read.wherePlus = parseWherePlus(tokenizer);
//        System.out.println("得到了wherePlus");
        return read;
    }
    private static WherePlus parseWherePlus(Tokenizer tokenizer) throws Exception {
        WherePlus wherePlus = new WherePlus();

        if(!"where".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        wherePlus.singleExp1 = exp1;//注意，这里面是包含别名的

        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {
            wherePlus.logicOp = logicOp;
            return wherePlus;
        }
        if(!isLogicOp(logicOp)) {
            throw Error.InvalidCommandException;
        }
        wherePlus.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        wherePlus.singleExp2 = exp2;

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return wherePlus;
    }
    //这个方法和parseWherePlus的唯一区别就是where已经从tokenizer中取出来了
    private static WherePlus parseWherePlus2(Tokenizer tokenizer) throws Exception {
        tokenizer.pop();
        WherePlus wherePlus = new WherePlus();
        SingleExpression exp1 = parseSingleExp(tokenizer);
        wherePlus.singleExp1 = exp1;//注意，这里面是包含别名的

        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {
            wherePlus.logicOp = logicOp;
            return wherePlus;
        }
        if(!isLogicOp(logicOp)) {
            throw Error.InvalidCommandException;
        }
        wherePlus.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        wherePlus.singleExp2 = exp2;

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return wherePlus;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while(true) {
                String field = tokenizer.peek();
                if(!isName(field)) {
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if(",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);

        if(!"from".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp) || " ".equals(tmp)) {
//            System.out.println("where可以为空");
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if(!"where".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if(!isLogicOp(logicOp)) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();
        
        String field = tokenizer.peek();
        if(!isName(field)) {
            throw Error.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if(!isCmpOp(op)) {
            throw Error.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        
        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    //解析create语句
    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if(!"table".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        Create create = new Create();
        String name = tokenizer.peek();
        if(!isName(name)) {
            throw Error.InvalidCommandException;
        }
        create.tableName = name;

        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if("(".equals(field)) {
                break;
            }

            if(!isName(field)) {
                throw Error.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if(!isType(fieldType)) {
                throw Error.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();
            
            String next = tokenizer.peek();
            if(",".equals(next)) {
                continue;
            } else if("".equals(next)) {
                throw Error.TableNoIndexException;
            } else if("(".equals(next)) {
                break;
            } else {
                throw Error.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        tokenizer.pop();
        if(!"index".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> indexes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if(")".equals(field)) {
                break;
            }
            if(!isName(field)) {
                throw Error.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if(!"".equals(tokenizer.peek()))   {
            throw Error.InvalidCommandException;
        }
        return create;
    }

    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
        "string".equals(tp));
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Abort();
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if(!"".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if("".equals(isolation)) {
            return begin;
        }
        if(!"isolation".equals(isolation)) {
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();
        String level = tokenizer.peek();
        if(!"level".equals(level)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("committed".equals(tmp2)) {
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else if("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if(!"".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                return begin;
            } else {
                throw Error.InvalidCommandException;
            }
        } else {
            throw Error.InvalidCommandException;
        }
    }

    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }
}
