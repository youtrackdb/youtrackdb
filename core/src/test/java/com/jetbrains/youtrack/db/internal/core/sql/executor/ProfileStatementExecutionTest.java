package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ProfileStatementExecutionTest extends DbTestBase {

  @Test
  public void testProfile() {
    db.createClass("testProfile");

    db.begin();
    db.command("insert into testProfile set name ='foo'");
    db.command("insert into testProfile set name ='bar'");
    db.commit();

    ResultSet result = db.query("PROFILE SELECT FROM testProfile WHERE name ='bar'");
    Assert.assertTrue(result.getExecutionPlan().get().prettyPrint(0, 2).contains("μs"));

    result.close();
  }
}
