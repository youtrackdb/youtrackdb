package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.math.BigDecimal;
import org.junit.Test;

public class BigDecimalQuerySupportTest extends DbTestBase {

  @Test
  public void testDecimalPrecision() {
    db.command("CREATE Class Test").close();
    db.command("CREATE Property Test.salary DECIMAL").close();
    db.begin();
    db.command(
            "INSERT INTO Test set salary = ?", new BigDecimal("179999999999.99999999999999999999"))
        .close();
    db.commit();
    try (ResultSet result = db.query("SELECT * FROM Test")) {
      BigDecimal salary = result.next().getProperty("salary");
      assertEquals(new BigDecimal("179999999999.99999999999999999999"), salary);
    }
  }
}
