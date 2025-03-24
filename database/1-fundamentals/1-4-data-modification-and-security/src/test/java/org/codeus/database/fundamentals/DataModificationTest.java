package org.codeus.database.fundamentals;

import java.util.List;
import java.util.Map;
import org.codeus.database.common.EmbeddedPostgreSqlSetup;
import org.junit.jupiter.api.Test;

public class DataModificationTest extends EmbeddedPostgreSqlSetup {

    @Test
    void test() throws Exception {
        final List<Map<String, Object>> result = executeQueryFromFile("queries/01_01_insert.sql");
        System.out.println(result.toString());
    }

}
