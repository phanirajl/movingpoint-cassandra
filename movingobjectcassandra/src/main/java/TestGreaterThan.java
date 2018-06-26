import java.util.Stack;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class TestGreaterThan {

    public static void main( String[] args ) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse("SELECT C1.name, C2.name" +
                " FROM City as C1, City as C2 WHERE direction(C2.center, C1.center) <=100");
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        System.out.println(plainSelect.getWhere() instanceof GreaterThanEquals);
        evaluate(plainSelect.getWhere().toString());
    }

    static void evaluate(String expr) throws JSQLParserException {
        final Stack<Long> stack = new Stack<Long>();
        System.out.println("expr=" + expr);
        Expression parseExpression = CCJSqlParserUtil.parseExpression(expr);
        System.out.println(parseExpression.toString());

    }
}