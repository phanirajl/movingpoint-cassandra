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
import java.lang.*;
import java.util.concurrent.TimeUnit;

public class QueryEngine {

    public static List<TableContent> tables = new ArrayList<TableContent>();
    public static Object[][] finalResult;
    private String server;
    private String keyspace;

    public QueryEngine(String serverAccessed, String keyspaceAccessed) {
        server = serverAccessed;
        keyspace = keyspaceAccessed;
        System.setProperty("com.datastax.driver.NATIVE_TRANSPORT_MAX_FRAME_SIZE_IN_MB", "2000");
        tables.clear();
    }

    public List<String> getColList(String table) {

        Cluster cluster = Cluster.builder().addContactPoint(server).build();
        Session session = cluster.connect(keyspace);

        List<ColumnMetadata> colmds = session.getCluster().getMetadata().getKeyspace(keyspace).getTable(table).getColumns();
        List<String> columns = new ArrayList<String>();
        for (int i=0; i<colmds.size(); i++) {
            columns.add(colmds.get(i).getName());
        }

        session.close();
        cluster.close();
        return columns;
    }

    public void originalQuery(String q) {
        Cluster cluster = Cluster.builder().addContactPoint(server).build();
        Session session = cluster.connect(keyspace);

//        Object[][] o = new Object[0][0];

        try {
            long startTime = System.nanoTime();
//            ResultSet results = session.executeAsync(q).get(3000000, TimeUnit.MILLISECONDS);
            ResultSet results = session.execute(new SimpleStatement(q).setReadTimeoutMillis(150000));
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;
            System.out.println(duration + " ms");
            System.out.println(results.all());

            session.close();
            cluster.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }


//        return o;
    }

    public MPQueryResult mpQuery(String q) {
        MPQueryResult res =  new MPQueryResult();
        try {
            long startOverall = System.nanoTime();
            if (q.contains("minus(")) {
                q = q.replaceAll("minus", "minusmp");
            }
            if (q.contains("union(")) {
                q = q.replaceAll("union", "unionmp");
            }
            Statement statement = CCJSqlParserUtil.parse(q);
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
                        tables.get(getIndexByTableName(tableAttribute[0], tables)).where.add(tableAttribute[1] + operandWhere[1] + operandWhere[2]);
                    }
                }
            }
            Cluster cluster = Cluster.builder().addContactPoint(server).build();
            Session session = cluster.connect(keyspace);
            for (int i=0; i<tables.size(); i++) {
                List<String> ls = new ArrayList<String>();
                ls.addAll(tables.get(i).attributes);
                if (tables.get(i).where == null) {
                    String query = "select " + String.join(",", ls) + " from " + tables.get(i).tableRealName + ";";
                    long startTime = System.nanoTime();
                    ResultSet results = session.execute(new SimpleStatement(query).setReadTimeoutMillis(150000));
                    long endTime = System.nanoTime();
                    long duration = (endTime - startTime) / 1000000;
//                    System.out.println(duration + " ms");
                    List<Row> lr = new ArrayList<Row>();
                    lr.addAll(results.all());
                    tables.get(i).content = lr;
                }
                else {
                    List<ColumnMetadata> lcm = cluster.getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable(tables.get(i).tableRealName).getPrimaryKey();
                    for (int j=0; j<lcm.size(); j++) {
                        tables.get(i).pkey.add(lcm.get(j).getName());
                    }
                    List<String> where = new ArrayList<String>();
                    where.addAll(tables.get(i).where);
                    String query = "select " + String.join(",", ls) + " from " + tables.get(i).tableRealName + " where " + String.join(" and ", where) + ";";
                    long startTime = System.nanoTime();
                    ResultSet results = session.execute(new SimpleStatement(query).setReadTimeoutMillis(150000));
                    long endTime = System.nanoTime();
                    long duration = (endTime - startTime) / 1000000;
//                    System.out.println(duration + " ms");
                    List<Row> lr = new ArrayList<Row>();
                    lr.addAll(results.all());
                    tables.get(i).content = lr;
                }
            }


            session.close();
            cluster.close();

            int row = 1;
            for (int i=0; i<tables.size(); i++) {
                row = row * tables.get(i).content.size();
            }

            Row[][] matrixRow = new Row[row][tables.size()];
            int divider = 1;
            for (int j=0; j<tables.size(); j++) {
                divider = divider * tables.get(j).content.size();
                for (int i=0; i<row; i++) {
                    matrixRow[i][j] = tables.get(j).content.get((i/(row/divider))%tables.get(j).content.size());
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
                finalResult = new Object[rowFR][plainSelect.getSelectItems().size()];

                for (int i=0; i<plainSelect.getSelectItems().size(); i++) {
                    if (plainSelect.getSelectItems().get(i).toString().contains("(")) {
                        int irowfr = 0;
                        for (int j=0; j<row; j++) {
                            if (selected[j] == true) {
                                finalResult[irowfr][i] = evaluate(plainSelect.getSelectItems().get(i).toString(), matrixRow[j]);
                                irowfr++;
                            }
                        }
                    }
                    else {
                        String[] s = plainSelect.getSelectItems().get(i).toString().split("\\.");
                        int irowfr = 0;
                        for (int j=0; j<row; j++) {
                            if (selected[j] == true) {
                                int index = getIndexByTableName(s[0], tables);
                                Row r = matrixRow[j][index];
                                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".line>")) {
                                    finalResult[irowfr][i] = convertUDTValueToLine(r, s[1]);
                                }
                                else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>")) {
                                    finalResult[irowfr][i] = convertUDTValueToPoint(r, s[1]);
                                }
                                else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>")) {
                                    finalResult[irowfr][i] = convertUDTValueToPoints(r, s[1]);
                                }
                                else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".mpoint>")) {
                                    finalResult[irowfr][i] = convertUDTValueToMPoint(r, s[1]);
                                }
                                else {
                                    finalResult[irowfr][i] = r.getObject(s[1]);
                                }
                                irowfr++;
                            }
                        }
                    }
                }

                for (int k=0; k<plainSelect.getSelectItems().size(); k++) {
                    System.out.print(plainSelect.getSelectItems().get(k) + " | ");
                }

                System.out.println();

                for (int j=0; j<rowFR; j++) {
                    for (int k=0; k<plainSelect.getSelectItems().size(); k++) {
                        if (finalResult[j][k]!=null) {
                            if (finalResult[j][k].getClass() == Point.class) {
                                res.indexPoint = k;
                                ((Point) finalResult[j][k]).print();
                            }
                            else if (finalResult[j][k].getClass() == Points.class) {
                                res.indexPoints = k;
                                ((Points) finalResult[j][k]).print();
                            }
                            else if (finalResult[j][k].getClass() == Line.class) {
                                res.indexLine = k;
                                ((Line) finalResult[j][k]).print();
                            }
                            else if (finalResult[j][k].getClass() == MPComponent.class) {
                                ((MPComponent) finalResult[j][k]).print();
                            }
                            else if (finalResult[j][k].getClass() == MPoint.class) {
                                ((MPoint) finalResult[j][k]).print();
                            }
                            else if (finalResult[j][k].getClass().toString().contains("[Ljava.util.Date")) {
                                Date[] d = (Date[]) finalResult[j][k];
                                System.out.print(d[0] + ", " + d[1] + " | ");
                            }
                            else {
                                System.out.print(finalResult[j][k] + " | ");
                            }
                        }


                    }
                    System.out.println();
                }
            }
            long endOverall = System.nanoTime();
            long durationOverall = (endOverall - startOverall) / 1000000;
            System.out.println(durationOverall + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
        res.indexPoint = -1;
        res.indexPoints = -1;
        res.indexLine = -1;
        if (finalResult.length>0) {
            for (int j=0; j<finalResult[0].length; j++) {
                if (finalResult[0][j] != null) {
                    if (finalResult[0][j].getClass() == Point.class) {
                        res.indexPoint = j;
                    }
                    else if (finalResult[0][j].getClass() == Points.class) {
                        res.indexPoints = j;
                    }
                    else if (finalResult[0][j].getClass() == Line.class) {
                        res.indexLine = j;
                    }
                }

            }
        }
        res.result = finalResult;

        return res;

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
            functionSplit[j] = functionSplit[j].replace("'", "");
            if (functionSplit[j].contains(".") && !isNumeric(functionSplit[j])) {
                String[] tableAttribute = functionSplit[j].split("\\.");
                tables.get(getIndexByTableName(tableAttribute[0], tables)).attributes.add(tableAttribute[1]);
            }
        }
    }

    public static Line convertUDTValueToLine(Row r, String attr) {
        Line l = new Line();
        l.point_set = new ArrayList<Point>();
        List<UDTValue> points = r.getUDTValue(attr).getList(2, UDTValue.class);
        for (int i=0; i<points.size(); i++) {
            Point p = new Point();
            p.absis = points.get(i).getDouble(0);
            p.ordinat = points.get(i).getDouble(1);
            l.point_set.add(p);
        }
        l.no_points = r.getUDTValue(attr).getInt(1);
        l.bounding_box[0] = new Point();
        l.bounding_box[1] = new Point();
        l.bounding_box[0].absis = r.getUDTValue(attr).getTupleValue(0).getUDTValue(0).getDouble(0);
        l.bounding_box[0].ordinat = r.getUDTValue(attr).getTupleValue(0).getUDTValue(0).getDouble(1);
        l.bounding_box[1].absis = r.getUDTValue(attr).getTupleValue(0).getUDTValue(1).getDouble(0);
        l.bounding_box[1].ordinat = r.getUDTValue(attr).getTupleValue(0).getUDTValue(1).getDouble(1);
        return l;
    }

    public static Point convertUDTValueToPoint(Row r, String attr) {
        Point p = new Point();
        p.absis = r.getUDTValue(attr).getDouble(0);
        p.ordinat = r.getUDTValue(attr).getDouble(1);
        return p;
    }

    public static Points convertUDTValueToPoints(Row r, String attr) {
        Points ps = new Points();
        ps.point_set = new HashSet<Point>();
        Set<UDTValue> points = r.getUDTValue(attr).getSet(2, UDTValue.class);
        for (UDTValue uv : points) {
            Point p = new Point();
            p.absis = uv.getDouble(0);
            p.ordinat = uv.getDouble(1);
            ps.point_set.add(p);
        }
        ps.no_points = r.getUDTValue(attr).getInt(1);
        ps.bounding_box[0] = new Point();
        ps.bounding_box[1] = new Point();
        ps.bounding_box[0].absis = r.getUDTValue(attr).getTupleValue(0).getUDTValue(0).getDouble(0);
        ps.bounding_box[0].ordinat = r.getUDTValue(attr).getTupleValue(0).getUDTValue(0).getDouble(1);
        ps.bounding_box[1].absis = r.getUDTValue(attr).getTupleValue(0).getUDTValue(1).getDouble(0);
        ps.bounding_box[1].ordinat = r.getUDTValue(attr).getTupleValue(0).getUDTValue(1).getDouble(1);
        return ps;
    }

    public static MPoint convertUDTValueToMPoint(Row r, String attr) {
        MPoint mp = new MPoint();
        mp.no_components = r.getUDTValue(attr).getInt(1);
        mp.component_set = new ArrayList<MPComponent>();
        mp.lifespan[0] = r.getUDTValue(attr).getTupleValue(2).getTimestamp(0);
        mp.lifespan[1] = r.getUDTValue(attr).getTupleValue(2).getTimestamp(1);
        List<UDTValue> mpoints = r.getUDTValue(attr).getList(3, UDTValue.class);
        for (UDTValue uv : mpoints) {
            MPComponent mpc = new MPComponent();
            mpc.p.absis = uv.getUDTValue(0).getDouble(0);
            mpc.p.ordinat = uv.getUDTValue(0).getDouble(1);
            mpc.t = uv.getTimestamp(1);
            mp.component_set.add(mpc);
        }

        mp.bounding_box[0] = new Point();
        mp.bounding_box[1] = new Point();
        mp.bounding_box[0].absis = r.getUDTValue(attr).getTupleValue(0).getUDTValue(0).getDouble(0);
        mp.bounding_box[0].ordinat = r.getUDTValue(attr).getTupleValue(0).getUDTValue(0).getDouble(1);
        mp.bounding_box[1].absis = r.getUDTValue(attr).getTupleValue(0).getUDTValue(1).getDouble(0);
        mp.bounding_box[1].ordinat = r.getUDTValue(attr).getTupleValue(0).getUDTValue(1).getDouble(1);
        return mp;
    }

    public static Object evaluate(String expr, final Row[] result) throws JSQLParserException {
        final Stack<Object> stack = new Stack<Object>();
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
//                System.out.println(name);
                CustomFunction cf = new CustomFunction();
                stack.push(cf.getFunction(name, stack, result, tables));
            }

        };
        StringBuilder b = new StringBuilder();
        deparser.setBuffer(b);
        parseExpression.accept(deparser);

        return stack.pop();
    }

    public static boolean isNumeric(String s) {
        try {
            double d = Double.parseDouble(s);
        } catch (NumberFormatException | NullPointerException nfe) {
            return false;
        }
        return true;
    }
}
