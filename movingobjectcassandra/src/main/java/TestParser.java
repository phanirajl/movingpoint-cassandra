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

public class TestParser {

    public static List<TableContent> tables = new ArrayList<TableContent>();
    public static void main(String args[]) {
        try {

            Statement statement = CCJSqlParserUtil.parse
                    ("select distance('C1.center', 'C2.center') from City as C1, City as C2 where C2.name='Yogyakarta' and C1.name='Bekasi';");
            Select selectStatement = (Select) statement;
            PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
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
                if (!plainSelect.getSelectItems().get(i).toString().contains("(")) {
                    if (plainSelect.getSelectItems().get(i).toString().contains(".")) {
                        String[] s = plainSelect.getSelectItems().get(i).toString().split("\\.");
                        for (int j=0; j<tables.size(); j++) {
                            if (s[0].equals(tables.get(j).tableName)) {
                                tables.get(j).attributes.add(s[1]);
                            }
                        }
                    }
                }
                else {
                    addAttributesFromFunction(plainSelect.getSelectItems().get(i).toString());
                }

            }

            Set<String> functionContainedInWhere = new HashSet<String>();

            if (plainSelect.getWhere() != null) {
                String[] whereParsed = plainSelect.getWhere().toString().split(" AND ");
                for (int i=0; i<whereParsed.length; i++) {
                    if (whereParsed[i].contains("(")) {
                        addAttributesFromFunction(whereParsed[i]);
                        functionContainedInWhere.add(whereParsed[i]);
                    }
                    else {
                        String[] operandWhere = whereParsed[i].split("((?<= = )|(?= = )|(?<= > )|(?= > )|(?<= < )|(?= < )|(?<= >= )|(?= >= ))|(?<= <= )|(?= <= )|(?<= != )|(?= != )");
                        String[] tableAttribute = operandWhere[0].split("\\.");
                        if (tables.get(getIndexByTableName(tableAttribute[0], tables)).where == null) {
                            tables.get(getIndexByTableName(tableAttribute[0], tables)).where = new HashSet<String>();
                        }
                        String tableRealName = tables.get(getIndexByTableName(tableAttribute[0], tables)).tableRealName;
                        tables.get(getIndexByTableName(tableAttribute[0], tables)).where.add(tableAttribute[1] + operandWhere[1] + operandWhere[2]);
                    }
                }
            }



            Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
            Session session = cluster.connect("moving_point");
            for (int i=0; i<tables.size(); i++) {
                List<String> ls = new ArrayList<String>();
                ls.addAll(tables.get(i).attributes);
                if (tables.get(i).where == null) {
                    String query = "select " + String.join(",", ls) + " from " + tables.get(i).tableRealName + ";";
                    System.out.println(query);
                    ResultSet results = session.execute(query);
                    List<Row> lr = new ArrayList<Row>();
                    lr.addAll(results.all());
                    tables.get(i).content = lr;
                }
                else {
                    List<String> where = new ArrayList<String>();
                    where.addAll(tables.get(i).where);
                    String query = "select " + String.join(",", ls) + " from " + tables.get(i).tableRealName + " where " + String.join(" and ", where) + ";";
                    System.out.println(query);
                    ResultSet results = session.execute(query);
                    List<Row> lr = new ArrayList<Row>();
                    lr.addAll(results.all());
                    tables.get(i).content = lr;
                }
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

            boolean[] selected = new boolean[row];

            for (int i=0; i<row; i++) {
                selected[i] = true;
            }

            if (functionContainedInWhere.size() > 0) {
                for (String s: functionContainedInWhere) {
                    String[] splitS = s.split("((?<= = )|(?= = )|(?<= > )|(?= > )|(?<= < )|(?= < )|(?<= >= )|(?= >= ))|(?<= <= )|(?= <= )|(?<= != )|(?= != )");

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
                            }
                        }
                    }
                    else if (splitS.length == 3) {
                        if (splitS[1].equals(" = ")) {
                            //ini belum
                            for (int i=0; i<row; i++) {
                                if (selected[i] == true) {
                                    String o = (String) evaluate(splitS[0], matrixRow[i]);
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
                                    if (o > Double.parseDouble(splitS[2])) {
                                        selected[i] = false;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            int rowFR = 0;
            for (int i=0; i<row; i++) {
                if (selected[i] == true) {
                    rowFR++;
                }
            }

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

            session.close();
            cluster.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static int getIndexByTableName (String s, List<TableContent> l) {
        for (int i=0; i<l.size(); i++) {
            if (l.get(i).tableName.equals(s)) {
                return i;
            }
        }
        return -1;
    }

    public static void addAttributesFromFunction (String s) {
        String[] operatorSplit = s.split("((?<= = )|(?= = )|(?<= > )|(?= > )|(?<= < )|(?= < )|(?<= >= )|(?= >= ))|(?<= <= )|(?= <= )|(?<= != )|(?= != )");
        String[] functionSplit = operatorSplit[0].split("\\, |\\s+|\\(|\\)");
        for (int j=0; j<functionSplit.length; j++) {
            if (functionSplit[j].contains(".")) {
                functionSplit[j] = functionSplit[j].replace("'", "");
                String[] tableAttribute = functionSplit[j].split("\\.");
                tables.get(getIndexByTableName(tableAttribute[0], tables)).attributes.add(tableAttribute[1]);
            }
        }
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
