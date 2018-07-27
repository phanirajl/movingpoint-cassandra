public class Test {
    public static void main(String[] args){
        Points ps1 = new Points();
        Point p = new Point();
        p.absis = 0.95655;
        p.ordinat = 0.4543;
        ps1.point_set.add(p);
        p.absis = 5.2425;
        p.ordinat = 0.48574;
        Points ps2 = new Points();
        p.absis = 5.2425;
        p.ordinat = 0.48574;
        for (Point p1 : ps2.point_set) {
            if (ps1.point_set.contains(p1)) {
                System.out.println("ada");
            }
        }
    }
}
