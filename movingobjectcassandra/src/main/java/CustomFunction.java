public class CustomFunction {
    public double distance (Point p1, Point p2) {
        int R = 6371; // Radius of the earth in km
        double dLat = deg2rad(p2.ordinat-p1.ordinat);  // deg2rad below
        double dLon = deg2rad(p2.absis-p1.absis);
        double a =
                Math.sin(dLat/2) * Math.sin(dLat/2) +
                        Math.cos(deg2rad(p1.ordinat)) * Math.cos(deg2rad(p2.ordinat)) *
                                Math.sin(dLon/2) * Math.sin(dLon/2)
                ;
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        double d = R * c; // Distance in km
        return d;
    }

    private double deg2rad(double deg) {
        return deg * (Math.PI/180);
    }

    public double length (Line l) {
        double result = 0;
        for (int i=1; i<l.no_points; i++) {
            result = result + distance(l.point_set.get(i-1), l.point_set.get(i));
        }
        return result;
    }
}
