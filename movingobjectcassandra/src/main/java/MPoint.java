import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;

public class MPoint {
    Point[] bounding_box = new Point[2];
    int no_components;
    Date[] lifespan = new Date[2];
    List<MPComponent> component_set = new ArrayList<MPComponent>();

    public void setBoundingBox() {
        Set<Double> absisList = new HashSet<Double>();
        Set<Double> ordinatList = new HashSet<Double>();
        for (MPComponent mpc : component_set) {
            absisList.add(mpc.p.absis);
            ordinatList.add(mpc.p.ordinat);
        }
        Point p = new Point();
        p.absis = Collections.min(absisList);
        p.ordinat = Collections.max(ordinatList);
        bounding_box[0] = p;

        p.absis = Collections.max(absisList);
        p.ordinat = Collections.max(ordinatList);
        bounding_box[1] = p;

    }

    public void setLifespan() {
        Set<Date> dateList = new HashSet<Date>();
        for (MPComponent mpc : component_set) {
            dateList.add(mpc.t);
        }
        Date d;
        d = Collections.min(dateList);
        lifespan[0] = d;
        d = Collections.max(dateList);
        lifespan[1] = d;
    }

    public void print() {
        System.out.print("[");
        for (MPComponent mpc : component_set) {
            System.out.print(mpc.p.absis + "," + mpc.p.ordinat + "," + mpc.t);
            System.out.print(";");
        }
        System.out.print("] | ");
    }

}


