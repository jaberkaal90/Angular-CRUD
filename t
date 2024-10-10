import java.util.List;
import java.util.Map;

@Service
public class JdbcBatchInsertMapService {

private final JdbcTemplate jdbcTemplate;
private static final int BATCH_SIZE = 1000; // Define batch size

public JdbcBatchInsertMapService(DataSource dataSource) {
this.jdbcTemplate = new JdbcTemplate(dataSource);
}

/**
* Inserts rows into the database in chunks to handle large datasets.
* @param tableName The name of the table to insert into
* @param rows List of rows where each row is a Map with column names as keys and values as data.
*/
public void batchInsertInChunks(String tableName, List<Map<String, String>> rows) {
int totalRecords = rows.size();
for (int i = 0; i < totalRecords; i += BATCH_SIZE) {
int end = Math.min(i + BATCH_SIZE, totalRecords);
List<Map<String, String>> batchList = rows.subList(i, end);
batchInsert(tableName, batchList); // Process each chunk
}
}

/**
* Inserts a batch of rows into the database where each row is represented by a Map.
* @param tableName The name of the table to insert into
* @param rows List of rows where each row is a Map with column names as keys and values as data.
*/
private void batchInsert(String tableName, List<Map<String, String>> rows) {
if (rows == null || rows.isEmpty()) {
return; // No rows to insert
}

// Get the column names from the first row's keys
Map<String, String> firstRow = rows.get(0);
String[] columns = firstRow.keySet().toArray(new String[0]);

// Build the SQL INSERT query dynamically based on the columns
StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
for (int i = 0; i < columns.length; i++) {
sql.append(columns[i]);
if (i < columns.length - 1) {
sql.append(", ");
}
}
sql.append(") VALUES (");
for (int i = 0; i < columns.length; i++) {
sql.append("?");
if (i < columns.length - 1) {
sql.append(", ");
}
}
sql.append(")");

// Execute the batch insert using JdbcTemplate
jdbcTemplate.batchUpdate(sql.toString(), new BatchPreparedStatementSetter() {

@Override
public void setValues(PreparedStatement ps, int i) throws SQLException {
Map<String, String> row = rows.get(i);
for (int j = 0; j < columns.length; j++) {
ps.setString(j + 1, row.get(columns[j])); // Set each column value
}
}

@Override
public int getBatchSize() {
return rows.size();
}
});
}
}