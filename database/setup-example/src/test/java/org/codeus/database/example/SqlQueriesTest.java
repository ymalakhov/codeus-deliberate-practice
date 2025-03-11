package org.codeus.database.example;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


public class SqlQueriesTest extends EmbeddedPostgreSqlSetup {

  // File paths relative to src/test/resources
  private static final String QUERIES_DIR = "queries/";

  @Test
  void testProjectSummary() throws IOException, SQLException {
    // Execute the query
    List<Map<String, Object>> results = executeQueryFromFile("project_summary.sql");

    // Verify results
    assertEquals(5, results.size(), "Should have 5 projects");

    // Verify project with most team members
    Map<String, Object> mobileApp = results.stream()
      .filter(row -> "Mobile App Development".equals(row.get("project_name")))
      .findFirst()
      .orElse(null);

    assertNotNull(mobileApp, "Mobile App Development project should exist");
    assertEquals(3, ((Number) mobileApp.get("employee_count")).intValue());
    assertEquals(91666.67, ((Number) mobileApp.get("budget_per_employee")).doubleValue(), 0.01);

    // Verify completed project
    Map<String, Object> annualAudit = results.stream()
      .filter(row -> "Annual Audit".equals(row.get("project_name")))
      .findFirst()
      .orElse(null);

    assertNotNull(annualAudit, "Annual Audit project should exist");
    assertEquals("COMPLETED", annualAudit.get("status"));
  }

  @Test
  void testDepartmentSalaryStats() throws IOException, SQLException {
    // Execute the query
    List<Map<String, Object>> results = executeQueryFromFile("department_salary_stats.sql");

    // Verify results
    assertEquals(4, results.size(), "Should have stats for 4 departments");

    // Verify department with highest average salary
    Map<String, Object> topDepartment = results.get(0); // Results are ordered by avg_salary DESC
    assertEquals("Finance", topDepartment.get("department_name"));

    // Find Engineering department stats
    Map<String, Object> engineering = results.stream()
      .filter(row -> "Engineering".equals(row.get("department_name")))
      .findFirst()
      .orElse(null);

    assertNotNull(engineering, "Engineering department should be in the results");
    assertEquals(2, ((Number) engineering.get("employee_count")).intValue());
    assertEquals(78500.00, ((Number) engineering.get("avg_salary")).doubleValue(), 0.01);
    assertEquals(75000.00, ((Number) engineering.get("min_salary")).doubleValue());
    assertEquals(82000.00, ((Number) engineering.get("max_salary")).doubleValue());
    assertEquals(157000.00, ((Number) engineering.get("total_salary_expense")).doubleValue());
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "project_summary.sql",
    "department_salary_stats.sql"
  })
  void testQuerySyntax(String queryFile) throws IOException {
    // This test just ensures the query can be executed without syntax errors
    assertDoesNotThrow(() -> executeQueryFromFile(queryFile));
  }

  private List<Map<String, Object>> executeQueryFromFile(String queryFileName) throws IOException, SQLException {
    String filePath = getResourcePath(QUERIES_DIR + queryFileName);
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

    return results;
  }
}
