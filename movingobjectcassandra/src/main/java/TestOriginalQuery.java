import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class TestOriginalQuery {
    public static void main(String args[]) {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect("moving_point");

        session.execute("COPY city2 FROM 'new.csv' WITH DELIMITER=',' AND HEADER=false");

        session.close();
        cluster.close();
    }

}
