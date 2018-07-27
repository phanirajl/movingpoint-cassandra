import com.opencsv.CSVReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.Writer;
import com.opencsv.CSVWriter;

public class AddCSVToDB {
    private static String file;
    private static String keyspace;
    private static String server;
    private static String table;

    public AddCSVToDB(String new_keyspace, String new_server, String new_table, String new_file) throws IOException {
        keyspace = new_keyspace;
        server = new_server;
        table = new_table;
        file = new_file;
    }

    public static void addToDB() throws IOException {

        Reader reader = Files.newBufferedReader(Paths.get(file));
        CSVReader csvReader = new CSVReader(reader, ';');
        QueryEngine qe = new QueryEngine(server, keyspace);
        List<String> columns = qe.getColList(table);
        // Reading Records One by One in a String array
        String[] nextRecord;
        while ((nextRecord = csvReader.readNext()) != null) {
            qe = new QueryEngine(server, keyspace);
            String query = "insert into " + table + "("+ String.join(",", columns) +") values (" + String.join(",", nextRecord) + ");";
//            System.out.println(query);
            System.out.println(query);
            qe.originalQuery(query);
        }
        System.out.println("done");


    }
}