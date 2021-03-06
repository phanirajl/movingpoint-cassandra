import java.util.*;


public class Line {
    Point[] bounding_box = new Point[2];
    int no_points;
    List<Point> point_set = new ArrayList<Point>();

    public void setBoundingBox() {
        Set<Double> absisList = new HashSet<Double>();
        Set<Double> ordinatList = new HashSet<Double>();
        for (int i=0; i<point_set.size(); i++) {
            absisList.add(point_set.get(i).absis);
            ordinatList.add(point_set.get(i).ordinat);
        }
        Point p1 = new Point();
        p1.absis = Collections.min(absisList);
        p1.ordinat = Collections.max(ordinatList);
        bounding_box[0] = p1;

        Point p2 = new Point();
        p2.absis = Collections.max(absisList);
        p2.ordinat = Collections.max(ordinatList);
        bounding_box[1] = p2;

    }

    public void print() {
        System.out.print("[");
        for (int i=0; i<point_set.size(); i++) {
            System.out.print(point_set.get(i).absis + "," + point_set.get(i).ordinat);
            System.out.print(";");
        }
        System.out.print("] | ");
    }
}
