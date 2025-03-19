package tuesday;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class WarmUpSqlQueriesTest {

    private static EmbeddedPostgres postgres;
    private static Connection connection;

    private static final String SCHEMA_FILE = "schema.sql";
    private static final String TEST_DATA_FILE = "test-data.sql";

    @Test
    @Order(1)
    void testSelectAllCustomerNamesEasyWarmup() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("warmup/1_select_all_customer_names_easy_warmup.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(6, results.size(), "Should be 6 customers");
        // You could add more specific assertions to check for individual names if needed
    }

    @Test
    @Order(2)
    void testSelectKyivCustomersOrderedByNameEasyWarmup() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("warmup/2_select_kyiv_customers_ordered_by_name_easy_warmup.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(2, results.size(), "Should be 2 customers from Kyiv");
        List<String> customerNames = results.stream()
                .map(row -> (String) row.get("customer_name"))
                .toList();
        assertTrue(customerNames.contains("Ivan Kyivsky"), "Should contain Ivan Kyivsky");
        assertTrue(customerNames.contains("Maria Kyivska"), "Should contain Maria Kyivska");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "warmup/1_select_all_customer_names_easy_warmup.sql",
            "warmup/2_select_kyiv_customers_ordered_by_name_easy_warmup.sql"
    })
    void testQuerySyntax(String queryFile) throws IOException {
        // This test just ensures the query can be executed without syntax errors
        assertDoesNotThrow(() -> executeQueryFromFile(queryFile));
    }


    @BeforeAll
    static void startDatabase() throws IOException {
        System.out.println("Starting embedded PostgreSQL...");
        postgres = EmbeddedPostgres.start();
        try {
            connection = postgres.getPostgresDatabase().getConnection();
            connection.setAutoCommit(false); // For transaction control
            System.out.println("PostgreSQL started successfully");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get database connection", e);
        }
    }

    @AfterAll
    static void stopDatabase() throws IOException {
        System.out.println("Stopping embedded PostgreSQL...");
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("Error closing connection: " + e.getMessage());
            }
        }

        if (postgres != null) {
            postgres.close();
            System.out.println("PostgreSQL stopped successfully");
        }
    }

    @BeforeEach
    void setupSchema() throws SQLException, IOException {
        System.out.println("Setting up database schema and test data...");
        // Start a transaction that will be rolled back after each test
        connection.setAutoCommit(false);

        // Clear any existing data
        clearDatabase();

        // Initialize database schema
        executeSqlFile(getResourcePath(SCHEMA_FILE));

        // Load test data
        executeSqlFile(getResourcePath(TEST_DATA_FILE));

        System.out.println("Setup complete");
    }

    private void clearDatabase() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;");
        }
    }

    private void executeSqlFile(String filePath) throws IOException, SQLException {
        Path path = Paths.get(filePath);
        String sql = Files.readString(path);

        try (Statement statement = connection.createStatement()) {
            // Execute each statement separately
            for (String query : sql.split(";")) {
                if (!query.trim().isEmpty()) {
                    statement.execute(query);
                }
            }
        }
    }

    private String getResourcePath(String resourceName) {
        // In a real application, you would use a resource loader
        // Here we're simplifying by using a relative path
        return "src/test/resources/" + resourceName;
    }

    private List<Map<String, Object>> executeQueryFromFile(String queryFileName) throws IOException, SQLException {
        String filePath = getResourcePath(queryFileName);
        String sql = Files.readString(Paths.get(filePath));

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = resultSet.getObject(i);
                    row.put(columnName, value);
                }
                results.add(row);
            }
        }
        connection.rollback(); // Rollback transaction after each test
        return results;
    }
}