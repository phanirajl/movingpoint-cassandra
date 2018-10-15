import java.sql.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Scanner;

public class QueryEngineR {
    public static List<TableContent> tables = new ArrayList<TableContent>();
    public static Object[][] finalResult;
    static private String host = "localhost";
    static private String database = "moving_point";

    public static void main(String[] args){
        try {
            Scanner sc = new Scanner(System.in);
            String initquery = sc.nextLine();
            sc = new Scanner(System.in);
            boolean isMO = sc.nextBoolean();
            do {

                System.out.println(initquery);
                if (isMO == true) {
                    if (initquery.contains("minus(")) {
                        initquery = initquery.replaceAll("minus", "minusmp");
                    }
                    if (initquery.contains("union(")) {
                        initquery = initquery.replaceAll("union", "unionmp");
                    }
                    Statement statement = CCJSqlParserUtil.parse(initquery);
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

                    String url = "jdbc:postgresql://" + host + "/" + database;

                    Connection conn = DriverManager.getConnection(url);
                    java.sql.Statement st = conn.createStatement();
//            long startTime = System.nanoTime();
//            ResultSet rs = st.executeQuery("SELECT id, track FROM gotrack where id = 38081");
//            long endTime = System.nanoTime();
//            long duration = (endTime - startTime) / 1000000;
//            System.out.println(duration + " ms");
//            while (rs.next()) {
//                System.out.print("Column 1 returned ");
//                Object o = rs.getArray(2);
//                System.out.println(o);
//            }


                    for (int i=0; i<tables.size(); i++) {
                        List<String> ls = new ArrayList<String>();
                        ls.addAll(tables.get(i).attributes);
                        if (tables.get(i).where == null) {
                            String query = "select " + String.join(",", ls) + " from " + tables.get(i).tableRealName + ";";
//                    System.out.println(query);
                            System.out.println(query);
//                    long startTime = System.nanoTime();
                            long startTime = System.nanoTime();
                            ResultSet results = st.executeQuery(query);
                            long endTime = System.nanoTime();
                            long duration = (endTime - startTime) / 1000000;
                            System.out.println(duration + " ms");
                            results.close();
//                    List<Row> lr = new ArrayList<Row>();
//                    lr.addAll(results.all());
//                    tables.get(i).content = lr;
                        }
                        else {
//                    List<ColumnMetadata> lcm = cluster.getMetadata().getKeyspace(session.getLoggedKeyspace()).getTable(tables.get(i).tableRealName).getPrimaryKey();
//                    for (int j=0; j<lcm.size(); j++) {
//                        tables.get(i).pkey.add(lcm.get(j).getName());
//                    }
                            List<String> where = new ArrayList<String>();
                            where.addAll(tables.get(i).where);
                            String query = "select " + String.join(",", ls) + " from " + tables.get(i).tableRealName + " where " + String.join(" and ", where) + ";";
//                    System.out.println(query);
                            System.out.println(query);
                            long startTime = System.nanoTime();
                            ResultSet results = st.executeQuery(query);
                            long endTime = System.nanoTime();
                            long duration = (endTime - startTime) / 1000000;
                            System.out.println(duration + " ms");
//                    List<Row> lr = new ArrayList<Row>();
//                    lr.addAll(results.all());
//                    tables.get(i).content = lr;
                            results.close();
                        }
                    }

                    st.close();
                    tables.clear();
                    sc = new Scanner(System.in);
                    initquery = sc.nextLine();
                    isMO = sc.nextBoolean();
                }
                else {
                    String url = "jdbc:postgresql://" + host + "/" + database;
                    Connection conn = DriverManager.getConnection(url);
                    java.sql.Statement st = conn.createStatement();

                    long startTime = System.nanoTime();
                    ResultSet results = st.executeQuery(initquery);
                    long endTime = System.nanoTime();
                    long duration = (endTime - startTime) / 1000000;
                    System.out.println(duration + " ms");
                    results.close();

                    sc = new Scanner(System.in);
                    initquery = sc.nextLine();
                    isMO = sc.nextBoolean();
                }

            } while (!initquery.equals("end"));



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
            functionSplit[j] = functionSplit[j].replace("'", "");
            if (functionSplit[j].contains(".") && !isNumeric(functionSplit[j])) {
                String[] tableAttribute = functionSplit[j].split("\\.");
                tables.get(getIndexByTableName(tableAttribute[0], tables)).attributes.add(tableAttribute[1]);
            }
        }
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
