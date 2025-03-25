package org.codeus.database.fundamentals;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.codeus.database.fundamentals.config.EmbeddedPostgreSqlModificationSetup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class DataModificationTest extends EmbeddedPostgreSqlModificationSetup {


    @Nested
    @Order(1)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class InsertTests {
        @Order(1)
        @DisplayName("Insert using columns")
        @Test
        void testInsert_withColumns() throws Exception {
            String taskFile = "queries/01_01_insert_with_columns.sql";
            setupSchema("schema.sql");
            final String sql = getSqlRequestFromFilePath(taskFile);
            if (!sql.contains("first_name")) {
                fail("Please add columns you want to insert values to the sql request explicitly.");
            }
            int result = executeModificationQueryFromFile(taskFile);
            assertEquals(1, result);
        }

        @Order(2)
        @DisplayName("Insert without columns list")
        @Test
        void testInsert_withoutColumns() throws Exception {
            String taskFile = "queries/01_02_insert_without_columns.sql";
            setupSchema("schema.sql", "test-data.sql");
            final String sql = getSqlRequestFromFilePath(taskFile);
            if (sql.contains("first_name")) {
                fail("Please write SQL request without column names.");
            }
            int result = executeModificationQueryFromFile(taskFile);
            assertEquals(1, result);
        }

        @Order(3)
        @DisplayName("Insert with conflict")
        @Test
        void testInsert_withConflict() throws Exception {
            String taskFile = "queries/01_03_insert_with_on_conflict.sql";
            setupSchema("schema.sql", "test-data.sql");
            final String sql = getSqlRequestFromFilePath(taskFile);
            int result = executeModificationQueryFromFile(taskFile);
            if (!sql.contains("ON CONFLICT")) {
                fail("Please use keyword ON CONFLICT to resolve the unique constraint.");
            }
            assertEquals(1, result, "No updates were executed. "
                + "Check whether you use DO NOTHING during conflict resolving and avoid it.");
        }

        @Order(4)
        @DisplayName("Insert with return")
        @Test
        void testInsert_withReturning() throws Exception {
            String taskFile = "queries/01_04_insert_with_returning.sql";
            setupSchema("schema.sql");
            final List<Map<String, Object>> result = executeQueryFromFile(taskFile);
            assertEquals(1, result.size(), "Check if you return anything after INSERT statement");

            final Map<String, Object> row = result.get(0);
            assertEquals(2, row.size(), "You should return only two fields.");
            assertTrue(row.containsKey("first_name"));
            assertTrue(row.containsKey("last_name"));
        }

        @Order(5)
        @DisplayName("Insert multiple values in one")
        @Test
        void testInsert_multipleValuesInOneRequest() throws Exception {
            String taskFile = "queries/01_05_insert_multiple_values_in_one_request.sql";
            setupSchema("schema.sql");
            int result = executeModificationQueryFromFile(taskFile);
            assertTrue(result > 2, "Please insert more than two values in one request");
        }
    }


    @Nested
    @Order(2)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class UpdateTests {

        @Order(1)
        @DisplayName("Update no existed row")
        @Test
        void testUpdate_nonExistentId() throws Exception {
            String taskFile = "queries/02_01_update_with_incorrect_id.sql";
            setupSchema("schema.sql", "test-data.sql");
            int result = executeModificationQueryFromFile(taskFile);
            System.out.println("Rows affected: " + result);
            assertEquals(0, result, "No row affected for non-existed id");
        }

        @Order(2)
        @DisplayName("Update without condition")
        @Test
        void testUpdate_withoutCondition() throws Exception {
            String taskFile = "queries/02_02_update_without_condition.sql";
            setupSchema("schema.sql", "test-data.sql");
            int result = executeModificationQueryFromFile(taskFile);
            System.out.println("Rows affected: " + result);
            assertEquals(8, result, "All rows should be affected.");
        }

        @Order(3)
        @DisplayName("Update with select")
        @Test
        void testUpdate_withSelect() throws Exception {
            String taskFile = "queries/02_03_update_with_select.sql";
            setupSchema("schema.sql", "test-data.sql");
            int result = executeModificationQueryFromFile(taskFile);
            System.out.println("Rows affected: " + result);
            assertEquals(4, result, "Four rows should be affected.");

            String sql = "SELECT salary FROM employees";
            final List<Map<String, Object>> maps = executeQuery(sql);
            final boolean lessThanFifty = maps.stream()
                .peek(map -> System.out.println("Salary:" + map.get("salary")))
                .map(map -> map.get("salary"))
                .map(String::valueOf)
                .map(BigDecimal::new)
                .anyMatch(salary -> salary.intValue() < 50000);
            assertFalse(lessThanFifty, "Not all salaries were changed");
        }

        @Order(4)
        @DisplayName("Update jsonb field")
        @Test
        void testUpdate_jsonbField() throws Exception {
            String taskFile = "queries/02_04_update_jsonb_field.sql";
            setupSchema("schema.sql", "test-data.sql");
            int result = executeModificationQueryFromFile(taskFile);
            System.out.println("Rows affected: " + result);
            assertEquals(1, result, "Only one field should be affected.");

            String sql = "SELECT challenge_task -> 'days' as days from challenges "
                + "WHERE challenge_name = 'algo-challenge'";
            final List<Map<String, Object>> response = executeQuery(sql);
            assertEquals(1, response.size());

            final Map<String, Object> row = response.get(0);
            String answer = String.valueOf(row.get("days"));
            assertEquals("10", answer,"Json key 'days' should have value: 10");
        }

        @Order(5)
        @DisplayName("Update in procedure")
        @Test
        void testUpdate_inProcedure() throws Exception {
            String taskFile = "queries/02_05_update_in_procedure.sql";
            setupSchema("schema.sql", "test-data.sql");
            executeModificationQueryFromFile(taskFile);

            String sql = "SELECT * FROM employees WHERE last_name = 'Patron'";
            final List<Map<String, Object>> response = executeQuery(sql);
            assertEquals(1, response.size());

            final Map<String, Object> row = response.get(0);
            String salary = String.valueOf(row.get("salary"));
            assertEquals("60000.00", salary);
        }

    }

    @AfterEach
    void tearDown() throws SQLException {
        clearDatabase();
    }

    private void setupSchema(String schemaFileName) throws SQLException, IOException {
        setupSchema(schemaFileName, null);
    }

    @Override
    protected void setupSchema(String schemaFileName, String dataFileName) throws SQLException, IOException {
        // Start a transaction that will be rolled back after each test
        connection.setAutoCommit(false);

        // Clear any existing data
        clearDatabase();

        // Initialize database schema
        System.out.println("Setting up database schema from: " + schemaFileName);
        executeSqlFile(getResourcePath(schemaFileName));

        if (dataFileName != null && !dataFileName.isBlank()) {
            // Load test data
            System.out.println("Setting up test data from: " + dataFileName);
            executeSqlFile(getResourcePath(dataFileName));
        }


        System.out.println("Setup complete");
    }

}
