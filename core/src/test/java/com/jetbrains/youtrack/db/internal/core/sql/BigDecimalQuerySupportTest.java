package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import java.math.BigDecimal;
import org.junit.Test;

public class BigDecimalQuerySupportTest extends DbTestBase {

  @Test
  public void testDecimalPrecision() {
    session.command("CREATE Class Test").close();
    session.command("CREATE Property Test.salary DECIMAL").close();
    session.begin();
    session.command(
            "INSERT INTO Test set salary = ?", new BigDecimal("179999999999.99999999999999999999"))
        .close();
    session.commit();
    try (var result = session.query("SELECT * FROM Test")) {
      BigDecimal salary = result.next().getProperty("salary");
      assertEquals(new BigDecimal("179999999999.99999999999999999999"), salary);
    }
  }
}
