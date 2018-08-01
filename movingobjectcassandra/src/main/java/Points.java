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
        Point p = new Point();
        p.absis = Collections.min(absisList);
        p.ordinat = Collections.min(ordinatList);
        bounding_box[0] = p;

        p.absis = Collections.max(absisList);
        p.ordinat = Collections.max(ordinatList);
        bounding_box[1] = p;

    }

    public void print() {
        System.out.print("[");
        for (Point p : point_set) {
            System.out.print(p.absis + "," + p.ordinat);
            System.out.print(";");
        }
        System.out.print("] | ");
    }
}
