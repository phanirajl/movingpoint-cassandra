import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.HashSet;
import com.datastax.driver.core.Row;
import java.text.SimpleDateFormat;

public class CustomFunction {
    public Object getFunction (String name, Stack<Object> stack, Row[] result, List<TableContent> tables) {
        if (name.equals("distance")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();
            Point p1 = new Point();
            Point p2 = new Point();

            if (o1.getClass() == String.class) {
                String[] s = o1.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>")) {
                    p1 = QueryEngine.convertUDTValueToPoint(r, s[1]);
                }
            }
            else {
                p1 = (Point) o1;
            }

            if (o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>")) {
                    p2 = QueryEngine.convertUDTValueToPoint(r, s[1]);
                }
            }
            else {
                p2 = (Point) o2;
            }
            return distance(p1,p2);
        }
        else if (name.equals("length")) {
            Object o = stack.pop();
            Line l = new Line();

            if (o.getClass() == String.class) {
                String[] s = o.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".line>")) {
                    l = QueryEngine.convertUDTValueToLine(r, s[1]);
                }
            }
            else {
                l = (Line) o;
            }
            return length(l);
        }
        else if (name.equals("val")) {
            Object o = stack.pop();
            return (val((MPComponent)o));

        }
        else if (name.equals("inst")) {
            Object o = stack.pop();
            return (inst((MPComponent)o));
        }
        else if (name.equals("intersection")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();

            if (o1.getClass() == String.class && o2.getClass() == String.class) {
                String[] s1 = o1.toString().split("\\.");
                int index1 = QueryEngine.getIndexByTableName(s1[0], tables);
                Row r1 = result[index1];
                String[] s2 = o2.toString().split("\\.");
                int index2 = QueryEngine.getIndexByTableName(s2[0], tables);
                Row r2 = result[index2];
                if (r1.getColumnDefinitions().getType(s1[1]).toString().contains(".point>") && r2.getColumnDefinitions().getType(s2[1]).toString().contains(".points>")) {
                    Point p = QueryEngine.convertUDTValueToPoint(r1, s1[1]);
                    Points ps = QueryEngine.convertUDTValueToPoints(r2, s2[1]);
                    return intersection(p, ps);
                }
                else if (r1.getColumnDefinitions().getType(s1[1]).toString().contains(".points>") && r2.getColumnDefinitions().getType(s2[1]).toString().contains(".point>")) {
                    Points ps = QueryEngine.convertUDTValueToPoints(r1, s1[1]);
                    Point p = QueryEngine.convertUDTValueToPoint(r2, s2[1]);
                    return intersection(ps, p);
                }
                else if (r1.getColumnDefinitions().getType(s1[1]).toString().contains(".points>") && r2.getColumnDefinitions().getType(s2[1]).toString().contains(".points>")) {
                    Points ps1 = QueryEngine.convertUDTValueToPoints(r1, s1[1]);
                    Points ps2 = QueryEngine.convertUDTValueToPoints(r2, s2[1]);
                    return intersection(ps1, ps2);
                }
            }
            else if (o1.getClass() == String.class && o2.getClass() != String.class){
                String[] s = o1.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>") && o2.getClass() == Points.class) {
                    Point p = QueryEngine.convertUDTValueToPoint(r, s[1]);
                    return intersection(p, (Points)o2);
                }
                else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o2.getClass() == Point.class) {
                    Points ps = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return intersection(ps, (Point)o2);
                }
                else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o2.getClass() == Points.class) {
                    Points ps1 = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return intersection(ps1, (Points)o2);
                }
            }
            else if (o1.getClass() != String.class && o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>") && o1.getClass() == Points.class) {
                    Point p = QueryEngine.convertUDTValueToPoint(r, s[1]);
                    return intersection(p, (Points) o1);
                } else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o1.getClass() == Point.class) {
                    Points ps = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return intersection(ps, (Point) o1);
                } else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o1.getClass() == Points.class) {
                    Points ps1 = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return intersection(ps1, (Points) o1);
                }
            }
        }
        else if (name.equals("intersects")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();
            Points ps1 = new Points();
            Points ps2 = new Points();
            if (o1.getClass() == String.class) {
                String[] s = o1.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>")) {
                    ps1 = QueryEngine.convertUDTValueToPoints(r, s[1]);
                }
            }
            else {
                ps1 = (Points) o1;
            }

            if (o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>")) {
                    ps2 = QueryEngine.convertUDTValueToPoints(r, s[1]);
                }
            }
            else {
                ps2 = (Points) o2;
            }
            return intersects(ps1,ps2);
        }
        if (name.equals("union")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();

            if (o1.getClass() == String.class && o2.getClass() == String.class) {
                String[] s1 = o1.toString().split("\\.");
                int index1 = QueryEngine.getIndexByTableName(s1[0], tables);
                Row r1 = result[index1];
                String[] s2 = o2.toString().split("\\.");
                int index2 = QueryEngine.getIndexByTableName(s2[0], tables);
                Row r2 = result[index2];
                if (r1.getColumnDefinitions().getType(s1[1]).toString().contains(".point>") && r2.getColumnDefinitions().getType(s2[1]).toString().contains(".points>")) {
                    Point p = QueryEngine.convertUDTValueToPoint(r1, s1[1]);
                    Points ps = QueryEngine.convertUDTValueToPoints(r2, s2[1]);
                    return union(p, ps);
                }
                else if (r1.getColumnDefinitions().getType(s1[1]).toString().contains(".points>") && r2.getColumnDefinitions().getType(s2[1]).toString().contains(".point>")) {
                    Points ps = QueryEngine.convertUDTValueToPoints(r1, s1[1]);
                    Point p = QueryEngine.convertUDTValueToPoint(r2, s2[1]);
                    return union(ps, p);
                }
                else if (r1.getColumnDefinitions().getType(s1[1]).toString().contains(".points>") && r2.getColumnDefinitions().getType(s2[1]).toString().contains(".points>")) {
                    Points ps1 = QueryEngine.convertUDTValueToPoints(r1, s1[1]);
                    Points ps2 = QueryEngine.convertUDTValueToPoints(r2, s2[1]);
                    return union(ps1, ps2);
                }
            }
            else if (o1.getClass() == String.class && o2.getClass() != String.class){
                String[] s = o1.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>") && o2.getClass() == Points.class) {
                    Point p = QueryEngine.convertUDTValueToPoint(r, s[1]);
                    return union(p, (Points)o2);
                }
                else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o2.getClass() == Point.class) {
                    Points ps = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return union(ps, (Point)o2);
                }
                else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o2.getClass() == Points.class) {
                    Points ps1 = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return union(ps1, (Points)o2);
                }
            }
            else if (o1.getClass() != String.class && o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>") && o1.getClass() == Points.class) {
                    Point p = QueryEngine.convertUDTValueToPoint(r, s[1]);
                    return union(p, (Points) o1);
                } else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o1.getClass() == Point.class) {
                    Points ps = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return union(ps, (Point) o1);
                } else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o1.getClass() == Points.class) {
                    Points ps1 = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return union(ps1, (Points) o1);
                }
            }
        }
        else if (name.equals("minus")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();

            if (o1.getClass() == String.class && o2.getClass() == String.class) {
                String[] s1 = o1.toString().split("\\.");
                int index1 = QueryEngine.getIndexByTableName(s1[0], tables);
                Row r1 = result[index1];
                String[] s2 = o2.toString().split("\\.");
                int index2 = QueryEngine.getIndexByTableName(s2[0], tables);
                Row r2 = result[index2];
                if (r1.getColumnDefinitions().getType(s1[1]).toString().contains(".point>") && r2.getColumnDefinitions().getType(s2[1]).toString().contains(".points>")) {
                    Point p = QueryEngine.convertUDTValueToPoint(r1, s1[1]);
                    Points ps = QueryEngine.convertUDTValueToPoints(r2, s2[1]);
                    return minus(ps, p);
                }
                else if (r1.getColumnDefinitions().getType(s1[1]).toString().contains(".points>") && r2.getColumnDefinitions().getType(s2[1]).toString().contains(".points>")) {
                    Points ps1 = QueryEngine.convertUDTValueToPoints(r1, s1[1]);
                    Points ps2 = QueryEngine.convertUDTValueToPoints(r2, s2[1]);
                    return minus(ps1, ps2);
                }
            }
            else if (o1.getClass() == String.class && o2.getClass() != String.class){
                String[] s = o1.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".point>") && o2.getClass() == Points.class) {
                    Point p = QueryEngine.convertUDTValueToPoint(r, s[1]);
                    return minus((Points)o2, p);
                }
                else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o2.getClass() == Points.class) {
                    Points ps = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return minus(ps, (Points)o2);
                }
            }
            else if (o1.getClass() != String.class && o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o1.getClass() == Point.class) {
                    Points ps = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return union(ps, (Point) o1);
                }
                else if (r.getColumnDefinitions().getType(s[1]).toString().contains(".points>") && o1.getClass() == Points.class) {
                    Points ps1 = QueryEngine.convertUDTValueToPoints(r, s[1]);
                    return union(ps1, (Points) o1);
                }
            }
        }
        else if (name.equals("crossings")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();
            Line l1 = new Line();
            Line l2 = new Line();

            if (o1.getClass() == String.class) {
                String[] s = o1.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".line>")) {
                    l1 = QueryEngine.convertUDTValueToLine(r, s[1]);
                }
            }
            else {
                l1 = (Line) o1;
            }

            if (o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".line>")) {
                    l2 = QueryEngine.convertUDTValueToLine(r, s[1]);
                }
            }
            else {
                l2 = (Line) o2;
            }
            return crossings(l1,l2);
        }
        else if (name.equals("deftime")) {
            Object o = stack.pop();
            MPoint mp = new MPoint();

            if (o.getClass() == String.class) {
                String[] s = o.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".mpoint>")) {
                    mp = QueryEngine.convertUDTValueToMPoint(r, s[1]);
                }
            }
            else {
                mp = (MPoint) o;
            }
            return deftime(mp);
        }
        else if (name.equals("locations")) {
            Object o = stack.pop();
            MPoint mp = new MPoint();

            if (o.getClass() == String.class) {
                String[] s = o.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".mpoint>")) {
                    mp = QueryEngine.convertUDTValueToMPoint(r, s[1]);
                }
            }
            else {
                mp = (MPoint) o;
            }
            return locations(mp);
        }
        else if (name.equals("trajectory")) {
            Object o = stack.pop();
            MPoint mp = new MPoint();

            if (o.getClass() == String.class) {
                String[] s = o.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".mpoint>")) {
                    mp = QueryEngine.convertUDTValueToMPoint(r, s[1]);
                }
            }
            else {
                mp = (MPoint) o;
            }
            return trajectory(mp);
        }
        else if (name.equals("atinstant")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();
            MPoint mp = new MPoint();
            Date d = new Date();

            if (o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".mpoint>")) {
                    mp = QueryEngine.convertUDTValueToMPoint(r, s[1]);
                }
            }
            else {
                mp = (MPoint) o2;
            }

            if (o1.getClass() == String.class) {
                if (o1.toString().contains(".")) {
                    String[] s = o1.toString().split("\\.");
                    int index = QueryEngine.getIndexByTableName(s[0], tables);
                    Row r = result[index];
                    d = r.getTimestamp(s[1]);
                }
                else {
                    String s = o1.toString();
                    try {
                        d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s);
                    }
                    catch (java.text.ParseException e) {
                        e.printStackTrace();
                    }

                }

            }
            else {
                d = (Date) o1;
            }
            return atinstant(mp,d);
        }
        else if (name.equals("atperiods")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();
            MPoint mp = new MPoint();
            Date[] d;

            if (o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".mpoint>")) {
                    mp = QueryEngine.convertUDTValueToMPoint(r, s[1]);
                }
            }
            else {
                mp = (MPoint) o2;
            }

            d = (Date[]) o1;

            return atperiods(mp,d);
        }
        else if (name.equals("present")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();
            MPoint mp = new MPoint();
            Date d = new Date();

            if (o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".mpoint>")) {
                    mp = QueryEngine.convertUDTValueToMPoint(r, s[1]);
                }
            }
            else {
                mp = (MPoint) o2;
            }

            if (o1.getClass() == String.class) {
                if (o1.toString().contains(".")) {
                    String[] s = o1.toString().split("\\.");
                    int index = QueryEngine.getIndexByTableName(s[0], tables);
                    Row r = result[index];
                    d = r.getTimestamp(s[1]);
                }
                else {
                    String s = o1.toString();
                    try {
                        d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s);
                    }
                    catch (java.text.ParseException e) {
                        e.printStackTrace();
                    }

                }
            }
            else {
                d = (Date) o1;
            }
            return present(mp,d);
        }
        else if (name.equals("passes")) {
            Object o1 = stack.pop();
            Object o2 = stack.pop();
            MPoint mp = new MPoint();
            Point p = new Point();

            if (o2.getClass() == String.class) {
                String[] s = o2.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                if (r.getColumnDefinitions().getType(s[1]).toString().contains(".mpoint>")) {
                    mp = QueryEngine.convertUDTValueToMPoint(r, s[1]);
                }
            }
            else {
                mp = (MPoint) o2;
            }

            if (o1.getClass() == String.class) {
                String[] s = o1.toString().split("\\.");
                int index = QueryEngine.getIndexByTableName(s[0], tables);
                Row r = result[index];
                p = QueryEngine.convertUDTValueToPoint(r, s[1]);
            }
            else {
                p = (Point) o1;
            }
            return passes(mp,p);
        }
        return null;
    }

    public double distance (Point p1, Point p2) {
        //arccos(sin(lat1) · sin(lat2) + cos(lat1) · cos(lat2) · cos(lon1 - lon2)) · R
        int R = 6371; // Radius of the earth in km
//        double dLat = deg2rad(p2.ordinat-p1.ordinat);  // deg2rad below
//        double dLon = deg2rad(p2.absis-p1.absis);
//        double a =
//                Math.sin(dLat/2) * Math.sin(dLat/2) +
//                        Math.cos(deg2rad(p1.ordinat)) * Math.cos(deg2rad(p2.ordinat)) *
//                                Math.sin(dLon/2) * Math.sin(dLon/2)
//                ;
//        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
//        double d = R * c; // Distance in km
//        return d;
        return Math.acos(Math.sin(Math.toRadians(p1.ordinat)) * Math.sin(Math.toRadians(p2.ordinat)) + Math.cos(Math.toRadians(p1.ordinat)) * Math.cos(Math.toRadians(p2.ordinat)) * Math.cos(Math.toRadians(p1.absis)-Math.toRadians(p2.absis))) * R;
    }

//    private double deg2rad(double deg) {
//        return deg * (Math.PI/180);
//    }

    public double length (Line l) {
        double result = 0;
        for (int i=1; i<l.no_points; i++) {
            result = result + distance(l.point_set.get(i-1), l.point_set.get(i));
        }
        return result;
    }

    public Point val(MPComponent mpc) {
        return mpc.p;
    }

    public Date inst(MPComponent mpc) {
        return mpc.t;
    }

    public Point intersection(Point p, Points ps) {
        if (p.absis >= ps.bounding_box[0].absis && p.absis <= ps.bounding_box[1].absis && p.ordinat >= ps.bounding_box[0].ordinat && p.ordinat <= ps.bounding_box[1].ordinat) {
            for (Point po : ps.point_set) {
                if (po.absis == p.absis && po.ordinat == p.ordinat) {
                    return p;
                }
            }
        }
        return null;
    }

    public Point intersection(Points ps, Point p) {
        return intersection(p,ps);
    }

    public Points intersection(Points ps1, Points ps2) {
        Points ps = new Points();
        if (ps1.bounding_box[0].absis >= ps2.bounding_box[0].absis && ps1.bounding_box[1].absis <= ps2.bounding_box[1].absis && ps1.bounding_box[0].ordinat >= ps2.bounding_box[0].ordinat && ps1.bounding_box[1].ordinat <= ps2.bounding_box[1].ordinat) {
            if (ps1.no_points < ps2.no_points) {
                for (Point p : ps1.point_set) {
                    for (Point po : ps2.point_set) {
                        if (po.absis == p.absis && po.ordinat == p.ordinat) {
                            ps.point_set.add(po);
                        }
                    }
                }
            }
            else {
                for (Point p : ps2.point_set) {
                    for (Point po : ps1.point_set) {
                        if (po.absis == p.absis && po.ordinat == p.ordinat) {
                            ps.point_set.add(po);
                        }
                    }
                }
            }
            ps.no_points=ps.point_set.size();
            ps.setBoundingBox();
        }
        return ps;
    }

    public boolean intersects(Points ps1, Points ps2) {
        if (ps1.bounding_box[0].absis >= ps2.bounding_box[0].absis && ps1.bounding_box[1].absis <= ps2.bounding_box[1].absis && ps1.bounding_box[0].ordinat >= ps2.bounding_box[0].ordinat && ps1.bounding_box[1].ordinat <= ps2.bounding_box[1].ordinat) {
            if (ps1.no_points < ps2.no_points) {
                for (Point p : ps1.point_set) {
                    for (Point po : ps2.point_set) {
                        if (po.absis == p.absis && po.ordinat == p.ordinat) {
                            return true;
                        }
                    }
                }
            }
            else {
                for (Point p : ps2.point_set) {
                    for (Point po : ps1.point_set) {
                        if (po.absis == p.absis && po.ordinat == p.ordinat) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    public Points union(Point p, Points ps) {
        ps.point_set.add(p);
        ps.setBoundingBox();
        ps.no_points = ps.point_set.size();
        return ps;
    }

    public Points union(Points ps, Point p) {
        return union(p,ps);
    }

    public Points union(Points ps1, Points ps2) {
        if (ps1.no_points < ps2.no_points) {
            ps2.point_set.addAll(ps1.point_set);
            ps2.setBoundingBox();
            ps2.no_points = ps2.point_set.size();
            return ps2;
        }
        else {
            ps1.point_set.addAll(ps2.point_set);
            ps1.setBoundingBox();
            ps1.no_points = ps1.point_set.size();
            return ps1;
        }
    }

    public Points minus(Points ps, Point p) {
        ps.point_set.remove(p);
        ps.setBoundingBox();
        ps.no_points = ps.point_set.size();
        return ps;
    }

    public Points minus(Points ps1, Points ps2) {
        ps1.point_set.removeAll(ps2.point_set);
        ps1.setBoundingBox();
        ps1.no_points = ps1.point_set.size();
        return ps1;
    }

    public Points crossings(Line l1, Line l2) {
        Points ps = new Points();
        if (l1.bounding_box[0].absis >= l2.bounding_box[0].absis && l1.bounding_box[1].absis <= l2.bounding_box[1].absis && l1.bounding_box[0].ordinat >= l2.bounding_box[0].ordinat && l1.bounding_box[1].ordinat <= l2.bounding_box[1].ordinat) {
            if (l1.no_points < l2.no_points) {
                for (Point p : l1.point_set) {
                    for (Point po : l2.point_set) {
                        if (po.absis == p.absis && po.ordinat == p.ordinat) {
                            ps.point_set.add(po);
                        }
                    }
                }
            }
            else {
                for (Point p : l2.point_set) {
                    for (Point po : l1.point_set) {
                        if (po.absis == p.absis && po.ordinat == p.ordinat) {
                            ps.point_set.add(po);
                        }
                    }
                }
            }
            ps.no_points=ps.point_set.size();
            ps.setBoundingBox();
        }
        return ps;
    }

    public Date[] deftime(MPoint mp) {
        return mp.lifespan;
    }

    public Points locations(MPoint mp) {
        Points ps = new Points();
        for (MPComponent mpc : mp.component_set) {
            ps.point_set.add(val(mpc));
        }
        ps.setBoundingBox();
        ps.no_points=ps.point_set.size();
        return ps;
    }

    public Line trajectory(MPoint mp) {
        Line l = new Line();
        for (MPComponent mpc : mp.component_set) {
            l.point_set.add(val(mpc));
        }
        l.setBoundingBox();
        l.no_points=l.point_set.size();
        return l;
    }

    public MPComponent atinstant(MPoint mp, Date d) {
//        System.out.println(mp.lifespan[0]);
//        System.out.println(mp.lifespan[1]);
        if (mp.lifespan[0].after(d) || mp.lifespan[1].before(d)) {
            return null;
        }
        else {
            for (MPComponent mpc : mp.component_set) {
                if (inst(mpc).equals(d)) {
                    return mpc;
                }
            }
        }
        return null;
    }

    public MPoint atperiods(MPoint mp, Date[] period) {
        if (mp.lifespan[0].after(period[1]) || mp.lifespan[1].before(period[0])) {
            return null;
        }
        else {
            MPoint result = new MPoint();
            for (MPComponent mpc : mp.component_set) {
                if (inst(mpc).after(period[0]) && inst(mpc).before(period[1])) {
                    result.component_set.add(mpc);
                }
            }
            result.setBoundingBox();
            result.setLifespan();
            return result;
        }
    }

    public boolean present(MPoint mp, Date d) {
        if (mp.lifespan[0].after(d) || mp.lifespan[1].before(d)) {
            return false;
        }
        else {
            for (MPComponent mpc : mp.component_set) {
                if (inst(mpc).equals(d)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean passes(MPoint mp, Point p) {
        if (p.absis >= mp.bounding_box[0].absis && p.absis <= mp.bounding_box[1].absis && p.ordinat >= mp.bounding_box[0].ordinat && p.ordinat <= mp.bounding_box[1].ordinat) {
            for (MPComponent mpc : mp.component_set) {
                if (val(mpc) == p) {
                    return true;
                }
            }
        }
        return false;
    }

}
