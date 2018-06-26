import java.util.*;
import com.datastax.driver.core.Row;

public class TableContent {
    String tableName;
    String tableRealName;
    Set<String> attributes;
    List<Row> content;
}
