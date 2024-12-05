package com.orientechnologies.orient.core.sql;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

public class SQLAlterClassTest extends DBTestBase {

  @Test
  public void alterClassRenameTest() {
    db.getMetadata().getSchema().createClass("TestClass");

    try {
      db.command("alter class TestClass name = 'test_class'").close();
      Assert.fail("the rename should fail for wrong syntax");
    } catch (YTCommandSQLParsingException ex) {

    }
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("TestClass"));
  }

  @Test
  public void testQuoted() {
    try {
      db.command("create class `Client-Type`").close();
      db.command("alter class `Client-Type` addcluster `client-type_usa`").close();

      db.begin();
      db.command("insert into `Client-Type` set foo = 'bar'").close();
      db.commit();

      YTResultSet result = db.query("Select from `Client-Type`");
      Assert.assertEquals(result.stream().count(), 1);
    } catch (YTCommandSQLParsingException ex) {
      Assert.fail();
    }
  }
}
