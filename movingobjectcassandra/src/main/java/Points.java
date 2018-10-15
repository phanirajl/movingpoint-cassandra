import java.util.*;

public class Points {
    Point[] bounding_box = new Point[2];
    int no_points;
    Set<Point> point_set = new HashSet<Point>();

    public void setBoundingBox() {
        Set<Double> absisList = new HashSet<Double>();
        Set<Double> ordinatList = new HashSet<Double>();
        for (Point p : point_set) {
            absisList.add(p.absis);
            ordinatList.add(p.ordinat);
        }
        Point p1 = new Point();
        p1.absis = Collections.min(absisList);
        p1.ordinat = Collections.min(ordinatList);
        bounding_box[0] = p1;

        Point p2 = new Point();
        p2.absis = Collections.max(absisList);
        p2.ordinat = Collections.max(ordinatList);
        bounding_box[1] = p2;

    }

    public void print() {
        System.out.print("[");
        for (Point p : point_set) {
            System.out.print(p.absis + "," + p.ordinat);
            System.out.print(";");
        }
        System.out.print("] | ");
    }

    public void removePoint(Point po) {

        for (Iterator<Point> iterator = point_set.iterator(); iterator.hasNext();) {
            Point p =  iterator.next();
            if (p.absis == po.absis && p.ordinat == po.ordinat) {
                iterator.remove();
            }
        }
    }


}
