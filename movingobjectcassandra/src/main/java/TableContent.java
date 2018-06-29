import java.util.*;
import com.datastax.driver.core.Row;

public class TableContent {
    String tableName;
    String tableRealName;
    Set<String> attributes;
    Set<String> where;
    Set<String> pkey = new HashSet<String>();
    List<Row> content;
}
