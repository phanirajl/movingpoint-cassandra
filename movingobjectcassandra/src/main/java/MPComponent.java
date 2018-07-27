import java.util.Date;

public class MPComponent {
    Point p = new Point();
    Date t;

    public void print() {
        System.out.println(p.absis + "," + p.ordinat + "," + t);
    }
}