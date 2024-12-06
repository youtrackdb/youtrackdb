package com.jetbrains.youtrack.db.internal.core.sql.functions.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.exception.QueryParsingException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import org.junit.Test;

public class CustomSQLFunctionsTest extends DbTestBase {

  @Test
  public void testRandom() {
    ResultSet result = db.query("select math_random() as random");
    assertTrue((Double) result.next().getProperty("random") > 0);
  }

  @Test
  public void testLog10() {
    ResultSet result = db.query("select math_log10(10000) as log10");
    assertEquals(4.0, result.next().getProperty("log10"), 0.0001);
  }

  @Test
  public void testAbsInt() {
    ResultSet result = db.query("select math_abs(-5) as abs");
    assertEquals(5, (int) (Integer) result.next().getProperty("abs"));
  }

  @Test
  public void testAbsDouble() {
    ResultSet result = db.query("select math_abs(-5.0d) as abs");
    assertEquals(5.0, result.findFirst().getProperty("abs"), 0.0);
  }

  @Test
  public void testAbsFloat() {
    ResultSet result = db.query("select math_abs(-5.0f) as abs");
    assertEquals(5.0f, result.findFirst().<Float>getProperty("abs"), 0.0);
  }

  @Test(expected = QueryParsingException.class)
  public void testNonExistingFunction() {
    db.query("select math_min('boom', 'boom') as boom").findFirst();
  }
}
