package com.orientechnologies.core.sql.executor;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OIfStatementExecutionTest extends DBTestBase {

  @Test
  public void testPositive() {
    YTResultSet results = db.command("if(1=1){ select 1 as a; }");
    Assert.assertTrue(results.hasNext());
    YTResult result = results.next();
    assertThat((Integer) result.getProperty("a")).isEqualTo(1);
    Assert.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testNegative() {
    YTResultSet results = db.command("if(1=2){ select 1 as a; }");
    Assert.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testIfReturn() {
    YTResultSet results = db.command("if(1=1){ return 'yes'; }");
    Assert.assertTrue(results.hasNext());
    Assert.assertEquals("yes", results.next().getProperty("value"));
    Assert.assertFalse(results.hasNext());
    results.close();
  }
}
