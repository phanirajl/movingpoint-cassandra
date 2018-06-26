import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.Stack;

public class ParseFunction {
    public static int getNumberOfParam(String name) {
        if (name.equals("direction")) {
            return 1;
        }
        else if (name.equals("distance")) {
            return 1;
        }
        else if (name.equals("present")) {
            return 2;
        }
        else if (name.equals("at")) {
            return 2;
        }
        else if (name.equals("period")) {
            return 2;
        }
        else if (name.equals("year")) {
            return 1;
        }
        return -1;
    }

//    public static Object getFunction(String name) {
//        if (name.equals("direction")) {
//            System.out.println("direction");
//            return 1;
//        }
//        else if (name.equals("distance")) {
//            System.out.println("distance");
//            return 7;
//        }
//        else if (name.equals("present")) {
//            System.out.println("present");
//            return 9;
//        }
//        else if (name.equals("at")) {
//            System.out.println("at");
//            return 0;
//        }
//        else if (name.equals("period")) {
//            System.out.println("periods");
//            return 0;
//        }
//        else if (name.equals("year")) {
//            System.out.println("year");
//            return 0;
//        }
//        return null;
//    }

//    public static void evaluate(String expr) throws JSQLParserException {
//        final Stack<Object> stack = new Stack<Object>();
//        System.out.println("expr=" + expr);
//        Expression parseExpression = CCJSqlParserUtil.parseExpression(expr);
//        ExpressionDeParser deparser = new ExpressionDeParser() {
//            @Override
//            public void visit(Addition addition) {
//                super.visit(addition);
//
//                long sum1 = Long.parseLong(stack.pop().toString());
//                long sum2 = Long.parseLong(stack.pop().toString());
//
//                stack.push(sum1 + sum2);
//            }
//
//            @Override
//            public void visit(Multiplication multiplication) {
//                super.visit(multiplication);
//
//                long fac1 = Long.parseLong(stack.pop().toString());
//                long fac2 = Long.parseLong(stack.pop().toString());
//
//                stack.push(fac1 * fac2);
//            }
//
//            @Override
//            public void visit(LongValue longValue) {
//                super.visit(longValue);
//                stack.push(longValue.getValue());
//            }
//
//            @Override
//            public void visit(StringValue stringValue) {
//                super.visit(stringValue);
//                stack.push(stringValue.getValue());
//            }
//
//            @Override
//            public void visit(Function function) {
//                super.visit(function);
//
//                String name = function.getName();
//                int params = function.getParameters().getExpressions().size();
//                for (int i=0; i<params; i++) {
//                    if (stack.pop().getClass() == String.class) {
//                        System.out.println("haha");
//                    }
//                }
//
//                stack.push(getFunction(name).toString());
//            }
//
//            @Override
//            public void visit(GreaterThanEquals greaterThanEquals) {
//                super.visit(greaterThanEquals);
//                System.out.println(greaterThanEquals.getLeftExpression().toString());
//                System.out.println(greaterThanEquals.getRightExpression().toString());
//                long v1 = Long.parseLong(stack.pop().toString());
//                long v2 = Long.parseLong(stack.pop().toString());
//
//                stack.push(v1 >= v2);
//            }
//
//        };
//        StringBuilder b = new StringBuilder();
//        deparser.setBuffer(b);
//        parseExpression.accept(deparser);
//
//        System.out.println(expr + " = " + stack.pop() );
//    }

    public static void main(String args[]) throws JSQLParserException {
//        evaluate("4+5*6");
//        evaluate("4*5+6");
//        evaluate("4 >= 1");
//        evaluate("distance('C2.center','C1.center')");

//        evaluate("direction('C2.center','C1.center') >= 80 ");
    }
}
