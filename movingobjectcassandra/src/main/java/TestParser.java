import com.datastax.driver.core.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import java.util.*;
//import net.sf.jsqlparser.util.TablesNamesFinder;

public class TestParser {
    public static List<TableContent> tables = new ArrayList<TableContent>();
    public static void main(String args[]) {
        try {
//            Statement statement = CCJSqlParserUtil.parse("SELECT length('R.stvalueline')" +
//                    " FROM Road as R WHERE R.name='C'");
            Statement statement = CCJSqlParserUtil.parse

                    ("SELECT C1.name, distance('C1.stvaluepoint', 'C2.stvaluepoint')\n" +
                    "FROM City as C1, City as C2\n" +
                    "WHERE C2.name = 'Bandung'");
            Select selectStatement = (Select) statement;
//            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
//            List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
//            System.out.println(tableList);
            PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
//            System.out.println(plainSelect.getSelectItems());
//            System.out.println(plainSelect.getFromItem());
//            System.out.println(plainSelect.getJoins());

            if (plainSelect.getFromItem().toString().contains(" AS ")) {
                TableContent tc = new TableContent();
                String[] s = plainSelect.getFromItem().toString().split(" AS ");
                tc.tableName = s[1];
                tc.tableRealName = s[0];
                tc.attributes = new HashSet<String>();
                tables.add(tc);
            }

            if (plainSelect.getJoins() != null) {
                for (int i=0; i<plainSelect.getJoins().size(); i++) {
                    if (plainSelect.getJoins().get(i).toString().contains(" AS ")) {
                        TableContent tc = new TableContent();
                        String[] s = plainSelect.getJoins().get(i).toString().split(" AS ");
                        tc.tableName = s[1];
                        tc.tableRealName = s[0];
                        tc.attributes = new HashSet<String>();
                        tables.add(tc);
                    }
                }
            }

            for (int i=0; i<plainSelect.getSelectItems().size(); i++) {
//                System.out.println(plainSelect.getSelectItems().get(i).toString());
                if (plainSelect.getSelectItems().get(i).toString().contains(".")) {
                    String[] s = plainSelect.getSelectItems().get(i).toString().split("\\.");
                    for (int j=0; j<tables.size(); j++) {
                        if (s[0].equals(tables.get(j).tableName)) {
                            tables.get(j).attributes.add(s[1]);
                        }
                    }
                }
            }

            Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
            Session session = cluster.connect("d_movingpoint");
            List<ColumnMetadata> lcm = cluster.getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable("all").getPrimaryKey();
            List<String> pkNames = new ArrayList<String>();
            for (int i=0; i<lcm.size(); i++) {
                pkNames.add(lcm.get(i).getName());
            }

            if (plainSelect.getWhere()!=null) {
                String[] whereParsed = plainSelect.getWhere().toString().split(" AND ");
                Set<String> functionContainedInWhere = new HashSet<String>();
                Set<String> tableSelectedFromWhere = new HashSet<String>();
                for (int i=0; i<whereParsed.length; i++) {
//                System.out.println(whereParsed[i]);
                    if (!whereParsed[i].contains("(")) {
                        if (whereParsed[i].contains(".")) {
                            String[] s = whereParsed[i].split("((?<= = )|(?= = )|(?<= > )|(?= > )|(?<= < )|(?= < )|(?<= >= )|(?= >= ))|(?<= <= )|(?= <= )|(?<= != )|(?= != )");
                            String[] subS = s[0].split("\\.");
                            if (!tableSelectedFromWhere.contains(subS[0])) {
                                tableSelectedFromWhere.add(subS[0]);
                                if (pkNames.contains(subS[1])) {
                                    String query = "update all set selectedas = selectedas + {'" + subS[0] + "'} where name=" + s[2] + " and kind='" + getTableRealNameByTableName(subS[0], tables) + "'";
//                                    System.out.println(query);
                                    session.execute(query);
//                                System.out.println(results.all());
                                }
                                else {
                                    String query = "select " + String.join(",", pkNames) + " from all where kind='" + getTableRealNameByTableName(subS[0], tables) + "' and " + subS[1] + s[1] + s[2] + " allow filtering";
//                                    System.out.println(query);
                                    session.execute(query);
                                    ResultSet results = session.execute(query);
                                    Set<String> names = new HashSet<String>();
                                    Set<String> kinds = new HashSet<String>();
                                    for (Row row : results) {
                                        names.add("'"+row.getString("name")+"'");
                                        kinds.add("'"+row.getString("kind")+"'");
                                    }

                                    query = "update all set selectedas= selectedas + {'" + subS[0] + "'} where name in (" + String.join(",",names) + ") and kind in (" + String.join(",",kinds) + ")";
//                                System.out.println(query);
                                    session.execute(query);
                                }
                            }
                            else {
                                String query = "select * from all where selectedas contains '"+ subS[0] +"' allow filtering";
                                session.execute(query);
                                ResultSet results = session.execute(query);
                                if (s[1].equals(" = ")) {
                                    for (Row row : results) {
                                        if (!row.getString(subS[1]).equals(s[2].replace("'", ""))) {
                                            query = "update all set selectedas=selectedas - {'" + subS[0] + "'} where name='" + row.getString("name") + "' and kind='" + row.getString("kind") + "'";
//                                        System.out.println(query);
                                            session.execute(query);
                                        }
                                    }
                                }
                                else if (s[1].equals(" > ")) {
                                    for (Row row : results) {
                                        if (Double.parseDouble(row.getObject(subS[1]).toString()) <= Double.parseDouble(s[2])) {
                                            query = "update all set selectedas=selectedas - {'" + subS[0] + "'} where name='" + row.getString("name") + "' and kind='" + row.getString("kind") + "'";
//                                        System.out.println(query);
                                            session.execute(query);
                                        }
                                    }
                                }
                                else if (s[1].equals(" < ")) {
                                    for (Row row : results) {
                                        if (Double.parseDouble(row.getObject(subS[1]).toString()) >= Double.parseDouble(s[2])) {
                                            query = "update all set selectedas=selectedas - {'" + subS[0] + "'} where name='" + row.getString("name") + "' and kind='" + row.getString("kind") + "'";
//                                        System.out.println(query);
                                            session.execute(query);
                                        }
                                    }
                                }
                                else if (s[1].equals(" >= ")) {
                                    for (Row row : results) {
                                        if (Double.parseDouble(row.getObject(subS[1]).toString()) < Double.parseDouble(s[2])) {
                                            query = "update all set selectedas=selectedas - {'" + subS[0] + "'} where name='" + row.getString("name") + "' and kind='" + row.getString("kind") + "'";
//                                        System.out.println(query);
                                            session.execute(query);
                                        }
                                    }
                                }
                                else if (s[1].equals(" <= ")) {
                                    for (Row row : results) {
                                        if (Double.parseDouble(row.getObject(subS[1]).toString()) > Double.parseDouble(s[2])) {
                                            query = "update all set selectedas=selectedas - {'" + subS[0] + "'} where name='" + row.getString("name") + "' and kind='" + row.getString("kind") + "'";
//                                        System.out.println(query);
                                            session.execute(query);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else {
                        functionContainedInWhere.add(whereParsed[i]);
                    }
                }
//                System.out.println(tableSelectedFromWhere);
                for (int i=0; i<tables.size(); i++) {
//                    System.out.println(tables.get(i).tableName);
//                    System.out.println(tables.get(i).tableRealName);
                    if (tableSelectedFromWhere.contains(tables.get(i).tableName)) {
                        String query = "select * from all where selectedas contains '"+ tables.get(i).tableName +"' allow filtering";
//                        System.out.println(query);
                        ResultSet results = session.execute(query);
                        List<Row> lr = new ArrayList<Row>();
                        lr.addAll(results.all());
                        tables.get(i).content = lr;
                    }
                    else {
                        String query = "select * from all where kind='" + tables.get(i).tableRealName + "' allow filtering";
//                        System.out.println(query);
                        ResultSet results = session.execute(query);
                        List<Row> lr = new ArrayList<Row>();
                        lr.addAll(results.all());
                        tables.get(i).content = lr;
                    }

                }

//                System.out.println(functionContainedInWhere);

                int row = 1;
                for (int i=0; i<tables.size(); i++) {
                    row = row * tables.get(i).content.size();
                }

                Row[][] matrixRow = new Row[row][tables.size()];
                for (int j=0; j<tables.size(); j++) {
                    for (int i=0; i<row; i++) {
                        matrixRow[i][j] = tables.get(j).content.get(i/(row/tables.get(j).content.size()));
                    }
                }

                boolean[] selected = new boolean[row];

                for (int i=0; i<row; i++) {
                    selected[i] = true;
                }

                if (functionContainedInWhere != null) {
                    for (String s: functionContainedInWhere) {
                        String[] splitS = s.split("((?<= = )|(?= = )|(?<= > )|(?= > )|(?<= < )|(?= < )|(?<= >= )|(?= >= ))|(?<= <= )|(?= <= )|(?<= != )|(?= != )");
//                    for (int i=0; i<splitS.length; i++) {
//                        System.out.println(splitS[i]);
//                    }
                        if (splitS.length == 1) {
                            //foreach row
                            //eval(splitS[0])
                            //kalau true
                            //kalau false
                            for (int i=0; i<row; i++) {
                                if (selected[i] == true) {
                                    boolean b = (Boolean) evaluate(splitS[0], matrixRow[i]);
                                    if (b == false) {
                                        selected[i] = false;
                                    }
//                                    System.out.println(o);
                                }
                            }
                        }
                        else if (splitS.length == 3) {
                            if (splitS[1].equals(" = ")) {
                                //ini belum
                                for (int i=0; i<row; i++) {
                                    if (selected[i] == true) {
                                        String o = (String) evaluate(splitS[0], matrixRow[i]);
//                                    System.out.println(o);
                                        if (!o.equals(splitS[2])) {
                                            selected[i] = false;
                                        }
                                    }
                                }
                            }
                            else if (splitS[1].equals(" > ")) {
                                for (int i=0; i<row; i++) {
                                    if (selected[i] == true) {
                                        double o = (Double) evaluate(splitS[0], matrixRow[i]);
//                                    System.out.println(o);
                                        if (o <= Double.parseDouble(splitS[2])) {
                                            selected[i] = false;
                                        }
                                    }
                                }
                            }
                            else if (splitS[1].equals(" < ")) {
                                for (int i=0; i<row; i++) {
                                    if (selected[i] == true) {
                                        double o = (Double) evaluate(splitS[0], matrixRow[i]);
//                                    System.out.println(o);
                                        if (o >= Double.parseDouble(splitS[2])) {
                                            selected[i] = false;
                                        }
                                    }
                                }
                            }
                            else if (splitS[1].equals(" >= ")) {
                                for (int i=0; i<row; i++) {
                                    if (selected[i] == true) {
                                        double o = (Double) evaluate(splitS[0], matrixRow[i]);
//                                    System.out.println(o);
                                        if (o < Double.parseDouble(splitS[2])) {
                                            selected[i] = false;
                                        }
                                    }
                                }
                            }
                            else if (splitS[1].equals(" <= ")) {
                                for (int i=0; i<row; i++) {
                                    if (selected[i] == true) {
                                        double o = (Double) evaluate(splitS[0], matrixRow[i]);
//                                    System.out.println(o);
                                        if (o > Double.parseDouble(splitS[2])) {
                                            selected[i] = false;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                else {
                    //kalau di where ga ada function
                    //kayaknya ga usah

                }

//                for (int i=0; i<row; i++) {
//                    System.out.println(selected[i]);
//                }

                int rowFR = 0;
                for (int i=0; i<row; i++) {
                    if (selected[i] == true) {
                        rowFR++;
                    }
                }


                //nih bagian select
                if (rowFR > 0) {
                    Object[][] finalResult = new Object[rowFR][plainSelect.getSelectItems().size()];

                    for (int i=0; i<plainSelect.getSelectItems().size(); i++) {
                        if (plainSelect.getSelectItems().get(i).toString().contains("(")) {
                            //for each eval ke yg selected = true
                            //masukin ke finalResult[][i]
                            int irowfr = 0;
                            for (int j=0; j<row; j++) {
                                if (selected[j] == true) {
                                    finalResult[irowfr][i] = evaluate(plainSelect.getSelectItems().get(i).toString(), matrixRow[j]);
                                    irowfr++;
                                }
                            }
                        }
                        else {
                            //masukin ke finalResult[][i] yang selected = true
                            String[] s = plainSelect.getSelectItems().get(i).toString().split("\\.");
                            int irowfr = 0;
                            for (int j=0; j<row; j++) {
                                if (selected[j] == true) {
                                    int index = getIndexByTableName(s[0], tables);
                                    Row r = matrixRow[j][index];
                                    finalResult[irowfr][i] = r.getObject(s[1]);
                                    irowfr++;
                                }
                            }
                        }
                    }

                    for (int j=0; j<rowFR; j++) {
                        for (int k=0; k<plainSelect.getSelectItems().size(); k++) {
                            System.out.print(finalResult[j][k] + " ");
                        }
                        System.out.println();
                    }
                }
            }
            else {
                //kalau ga ada where di query
                //langsung ambil semua yang di from
                for (int i=0; i<tables.size(); i++) {
                    String query = "select * from all where kind='" + tables.get(i).tableRealName + "' allow filtering";
                    ResultSet results = session.execute(query);
                    List<Row> lr = new ArrayList<Row>();
                    lr.addAll(results.all());
                    tables.get(i).content = lr;
                }

                int row = 1;
                for (int i=0; i<tables.size(); i++) {
                    row = row * tables.get(i).content.size();
                }

                Row[][] matrixRow = new Row[row][tables.size()];
                for (int j=0; j<tables.size(); j++) {
                    for (int i=0; i<row; i++) {
                        matrixRow[i][j] = tables.get(j).content.get(i/(row/tables.get(j).content.size()));
                    }
                }


                Object[][] finalResult = new Object[row][plainSelect.getSelectItems().size()];

                for (int i=0; i<plainSelect.getSelectItems().size(); i++) {
                    if (plainSelect.getSelectItems().get(i).toString().contains("(")) {
                        //for each eval ke yg selected = true
                        //masukin ke finalResult[][i]
                        finalResult[row][i] = evaluate(plainSelect.getSelectItems().get(i).toString(), matrixRow[row]);
                    }
                    else {
                        //masukin ke finalResult[][i] yang selected = true
                        String[] s = plainSelect.getSelectItems().get(i).toString().split("\\.");
                        for (int j=0; j<row; j++) {
                            int index = getIndexByTableName(s[0], tables);
                            Row r = matrixRow[j][index];
                            finalResult[row][i] = r.getObject(s[1]);
                        }

                        for (int j=0; j<row; j++) {
                            for (int k=0; k<plainSelect.getSelectItems().size(); k++) {
                                System.out.print(finalResult[j][k] + " ");
                            }
                            System.out.println();
                        }
                    }
                }

            }

            session.close();
            cluster.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static String getTableRealNameByTableName (String s, List<TableContent> l) {
        for (int i=0; i<l.size(); i++) {
            if (l.get(i).tableName.equals(s)) {
                return l.get(i).tableRealName;
            }
        }
        return null;
    }

    public static int getIndexByTableName (String s, List<TableContent> l) {
        for (int i=0; i<l.size(); i++) {
            if (l.get(i).tableName.equals(s)) {
                return i;
            }
        }
        return -1;
    }

    public static Object evaluate(String expr, final Row[] result) throws JSQLParserException {
        final Stack<Object> stack = new Stack<Object>();
//        System.out.println("expr=" + expr);
        Expression parseExpression = CCJSqlParserUtil.parseExpression(expr);
        ExpressionDeParser deparser = new ExpressionDeParser() {
            @Override
            public void visit(Addition addition) {
                super.visit(addition);

                long sum1 = Long.parseLong(stack.pop().toString());
                long sum2 = Long.parseLong(stack.pop().toString());

                stack.push(sum1 + sum2);
            }

            @Override
            public void visit(Multiplication multiplication) {
                super.visit(multiplication);

                long fac1 = Long.parseLong(stack.pop().toString());
                long fac2 = Long.parseLong(stack.pop().toString());

                stack.push(fac1 * fac2);
            }

            @Override
            public void visit(LongValue longValue) {
                super.visit(longValue);
                stack.push(longValue.getValue());
            }

            @Override
            public void visit(StringValue stringValue) {
                super.visit(stringValue);
                stack.push(stringValue.getValue());
            }

            @Override
            public void visit(Function function) {
                super.visit(function);

                String name = function.getName();

                if (name.equals("distance")) {
                    Object o1 = stack.pop();
                    Object o2 = stack.pop();
                    Point p1 = new Point();
                    Point p2 = new Point();

                    if (o1.getClass() == String.class) {
                        String[] s = o1.toString().split("\\.");
                        int index = getIndexByTableName(s[0], tables);
                        Row r = result[index];
                        if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>")) {
                            p1.absis = r.getUDTValue(s[1]).getDouble(0);
                            p1.ordinat = r.getUDTValue(s[1]).getDouble(1);
                        }
                    }

                    if (o2.getClass() == String.class) {
                        String[] s = o2.toString().split("\\.");
                        int index = getIndexByTableName(s[0], tables);
                        Row r = result[index];
                        if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>")) {
                            p2.absis = r.getUDTValue(s[1]).getDouble(0);
                            p2.ordinat = r.getUDTValue(s[1]).getDouble(1);
                        }
                    }

                    CustomFunction cf = new CustomFunction();
                    stack.push(cf.distance(p1, p2));
                }
                else if (name.equals("length")) {
                    Object o = stack.pop();
                    Line l = new Line();

                    if (o.getClass() == String.class) {
                        String[] s = o.toString().split("\\.");
                        int index = getIndexByTableName(s[0], tables);
                        Row r = result[index];
                        if (r.getColumnDefinitions().getType(s[1]).toString().contains(".line>")) {
                            l.no_points = r.getUDTValue(s[1]).getInt(1);
                            l.point_set = new ArrayList<Point>();
                            List<UDTValue> points = r.getUDTValue(s[1]).getList(2, UDTValue.class);
                            for (int i=0; i<points.size(); i++) {
                                Point p = new Point();
                                p.absis = points.get(i).getDouble(0);
                                p.ordinat = points.get(i).getDouble(1);
                                l.point_set.add(p);
                            }
                        }
                    }
                    CustomFunction cf = new CustomFunction();
                    stack.push(cf.length(l));
                }


            }

        };
        StringBuilder b = new StringBuilder();
        deparser.setBuffer(b);
        parseExpression.accept(deparser);

        return stack.pop();
    }

}
