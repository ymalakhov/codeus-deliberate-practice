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
public class SqlQueriesTest {

    private static EmbeddedPostgres postgres;
    private static Connection connection;

    private static final String SCHEMA_FILE = "schema.sql";
    private static final String TEST_DATA_FILE = "test-data.sql";

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

    @Order(3)
    @Test
    void testSelectCustomersKyivLviv() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/select_customers_kyiv_lviv.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(3, results.size(), "Should be 3 customers from Kyiv or Lviv");

        List<String> customerNames = results.stream()
                .map(row -> (String) row.get("customer_name"))
                .toList();
        assertTrue(customerNames.contains("Ivan Kyivsky"), "Should contain Ivan Kyivsky");
        assertTrue(customerNames.contains("Petro Lvivsky"), "Should contain Petro Lvivsky");
        assertTrue(customerNames.contains("Maria Kyivska"), "Should contain Maria Kyivska");
    }

    @Order(9)
    @Test
    void testSelectCustomersMAndA() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/select_customers_m_and_a.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(2, results.size(), "Should be 2 customers names start with 'M' and contain 'а'");

        List<String> customerNames = results.stream()
                .map(row -> (String) row.get("customer_name"))
                .toList();
        assertTrue(customerNames.contains("Maria Kyivska"), "Should contain Maria Kyivska");
        assertTrue(customerNames.contains("Maksym Kharkivsky"), "Should contain Maksym Kharkivsky");
    }

    @Order(10)
    @Test
    void testSelectCustomersUkraineNotKharkiv() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/select_customers_ukraine_not_kharkiv.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(4, results.size(), "Should be 4 customers from Ukraine not Kharkiv");
        List<String> customerNames = results.stream()
                .map(row -> (String) row.get("customer_name"))
                .toList();
        assertTrue(customerNames.contains("Ivan Kyivsky"), "Should contain Ivan Kyivsky");
        assertTrue(customerNames.contains("Petro Lvivsky"), "Should contain Petro Lvivsky");
        assertTrue(customerNames.contains("Maria Kyivska"), "Should contain Maria Kyivska");
        assertTrue(customerNames.contains("Olena Dniprovska"), "Should contain Olena Dniprovska");
    }

    @Order(4)
    @Test
    void testSelectUniqueCustomerCitiesSorted() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/select_unique_customer_cities_sorted.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(4, results.size(), "Should be 4 unique cities");
        List<String> cities = results.stream()
                .map(row -> (String) row.get("city"))
                .toList();
        assertEquals(List.of("Dnipro", "Kharkiv", "Kyiv", "Lviv"), cities, "Cities should be sorted alphabetically");
    }

    @Order(5)
    @Test
    void testSelectSavingsAccountsBalanceBetween() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/select_savings_accounts_balance_between.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(2, results.size(), "Should be 2 savings accounts with balance between 10000 and 50000");

        List<String> accountNumbers = results.stream()
                .map(row -> (String) row.get("account_number"))
                .toList();
        assertTrue(accountNumbers.contains("SA002"), "Should contain SA002");
        assertTrue(accountNumbers.contains("SA003"), "Should contain SA003");
    }

    @Order(6)
    @Test
    void testSelectCurrentAccountsBalanceOutOfRange() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/select_current_accounts_balance_out_of_range.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(2, results.size(), "Should be 2 current accounts with balance out of range");

        List<String> accountNumbers = results.stream()
                .map(row -> (String) row.get("account_number"))
                .toList();
        assertTrue(accountNumbers.contains("CA001"), "Should contain CA001");
        assertTrue(accountNumbers.contains("CA004"), "Should contain CA004");
    }

    @Order(11)
    @Test
    void testSelectAccountsSortedByTypeAndBalance() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/select_accounts_sorted_by_type_and_balance.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(6, results.size(), "Should be 6 accounts");

        List<String> accountNumbers = results.stream().map(row -> (String) row.get("account_number")).toList();
        List<String> expectedOrder = List.of("SA003", "SA002", "SA001", "CA004", "CA002", "CA001");
        assertEquals(expectedOrder, accountNumbers, "Accounts should be sorted by type and balance DESC");
    }


    @ParameterizedTest
    @ValueSource(strings = {
            "main/select_customers_kyiv_lviv.sql",
            "main/select_customers_m_and_a.sql",
            "main/select_customers_ukraine_not_kharkiv.sql",
            "main/select_unique_customer_cities_sorted.sql",
            "main/select_savings_accounts_balance_between.sql",
            "main/select_current_accounts_balance_out_of_range.sql",
            "main/select_accounts_sorted_by_type_and_balance.sql",
            "main/list_customers_with_checking_accounts_base.sql",
            "main/find_transactions_above_10000_sorted_by_date_base.sql",
            "main/search_transaction_descriptions_like_base.sql",
            "main/search_transaction_descriptions_ilike_base.sql",
            "main/search_transaction_deposit_iliike_base.sql",
            "main/search_transaction_draw_like_base.sql",
            "main/customers_with_savings_accounts_kyiv_lviv_base.sql",
            "main/high_value_transactions_september_2023_base.sql"
    })
    void testQuerySyntax(String queryFile) throws IOException {
        // This test just ensures the query can be executed without syntax errors
        assertDoesNotThrow(() -> executeQueryFromFile(queryFile));
    }

    @Order(7)
    @Test
    void testListCustomersWithCheckingAccountsBase() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/list_customers_with_checking_accounts_base.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(3, results.size(), "Should be 3 customers with checking accounts");
        List<String> customerNames = results.stream()
                .map(row -> (String) row.get("customer_name"))
                .toList();
        assertTrue(customerNames.contains("Ivan Kyivsky"), "Should contain Ivan Kyivsky");
        assertTrue(customerNames.contains("Petro Lvivsky"), "Should contain Petro Lvivsky");
        assertTrue(customerNames.contains("Olena Dniprovska"), "Should contain Olena Dniprovska");
    }

    @Order(8)
    @Test
    void testFindTransactionsAbove10000SortedByDateBase() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/find_transactions_above_10000_sorted_by_date_base.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(1, results.size(), "Should be 1 transaction above 10000");
        assertEquals("2023-10-15", results.get(0).get("transaction_date").toString(), "Transaction date should be 2023-10-15");
    }

    @Order(12)
    @Test
    void testSearchTransactionDescriptionsLikeBase() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/search_transaction_descriptions_like_base.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(3, results.size(), "Should be 3 transactions with type starting with 'deposit' (case-sensitive)");
        List<String> transactionTypes = results.stream()
                .map(row -> (String) row.get("transaction_type"))
                .toList();
        assertTrue(transactionTypes.contains("deposit"), "Should contain 'deposit'");
        assertTrue(transactionTypes.contains("deposit"), "Should contain 'deposit' (again)");
        assertTrue(transactionTypes.contains("deposit"), "Should contain 'deposit' (a third time)");
    }

    @Order(13)
    @Test
    void testSearchTransactionDescriptionsIlikeBase() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/search_transaction_descriptions_ilike_base.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(3, results.size(), "Should be 3 transactions with type containing 'deposit' (case-insensitive)");
        List<String> transactionTypes = results.stream()
                .map(row -> (String) row.get("transaction_type"))
                .toList();
        assertTrue(transactionTypes.contains("deposit"), "Should contain 'deposit'");
        assertTrue(transactionTypes.contains("deposit"), "Should contain 'deposit' (again)");
        assertTrue(transactionTypes.contains("deposit"), "Should contain 'deposit' (a third time)");
    }

    @Order(14)
    @Test
    void testSearchTransactionDepositIlikeBase() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/search_transaction_deposit_iliike_base.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(3, results.size(), "Should be 3 transactions with type starting with 'deposit' (case-insensitive)");
        List<String> transactionTypes = results.stream()
                .map(row -> (String) row.get("transaction_type"))
                .toList();
        assertTrue(transactionTypes.contains("deposit"), "Should contain 'deposit'");
        assertTrue(transactionTypes.contains("deposit"), "Should contain 'deposit' (again)");
        assertTrue(transactionTypes.contains("deposit"), "Should contain 'deposit' (a third time)");
    }

    @Order(15)
    @Test
    void testSearchTransactionDrawLikeBase() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/search_transaction_draw_like_base.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(2, results.size(), "Should be 2 transactions with type containing 'draw' (case-sensitive)");
        List<String> transactionTypes = results.stream()
                .map(row -> (String) row.get("transaction_type"))
                .toList();
        assertTrue(transactionTypes.contains("withdrawal"), "Should contain 'withdrawal'");
        assertTrue(transactionTypes.contains("withdrawal"), "Should contain 'withdrawal' (again)");
    }

    @Order(16)
    @Test
    void testCustomersWithSavingsAccountsKyivLvivBase() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/customers_with_savings_accounts_kyiv_lviv_base.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(3, results.size(), "Should be 3 customers with savings accounts in Kyiv or Lviv");
        List<String> customerNames = results.stream()
                .map(row -> (String) row.get("customer_name"))
                .toList();
        assertTrue(customerNames.contains("Ivan Kyivsky"), "Should contain Ivan Kyivsky");
        assertTrue(customerNames.contains("Petro Lvivsky"), "Should contain Petro Lvivsky");
        assertTrue(customerNames.contains("Maria Kyivska"), "Should contain Maria Kyivska");
    }

    @Order(17)
    @Test
    void testHighValueTransactionsSeptember2023Base() throws IOException, SQLException {
        List<Map<String, Object>> results = executeQueryFromFile("main/high_value_transactions_september_2023_base.sql");
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(1, results.size(), "Should be 1 high value transaction in September 2023");
        assertEquals("10000.00", results.get(0).get("amount").toString(), "Amount should be 10000.00");
        assertEquals("2023-09-10", results.get(0).get("transaction_date").toString(), "Transaction date should be 2023-09-10");
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