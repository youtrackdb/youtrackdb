package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OProfileStatementExecutionTest extends DBTestBase {

  @Test
  public void testProfile() {
    db.createClass("testProfile");

    db.begin();
    db.command("insert into testProfile set name ='foo'");
    db.command("insert into testProfile set name ='bar'");
    db.commit();

    YTResultSet result = db.query("PROFILE SELECT FROM testProfile WHERE name ='bar'");
    Assert.assertTrue(result.getExecutionPlan().get().prettyPrint(0, 2).contains("μs"));

    result.close();
  }
}
