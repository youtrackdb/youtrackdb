package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import org.junit.Assert;
import org.junit.Test;

public class SQLAlterClassTest extends DbTestBase {

  @Test
  public void alterClassRenameTest() {
    db.getMetadata().getSchema().createClass("TestClass");

    try {
      db.command("alter class TestClass name = 'test_class'").close();
      Assert.fail("the rename should fail for wrong syntax");
    } catch (CommandSQLParsingException ex) {

    }
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("TestClass"));
  }

  @Test
  public void testQuoted() {
    try {
      db.command("create class `Client-Type`").close();
      db.command("alter class `Client-Type` add_cluster `client-type_usa`").close();

      db.begin();
      db.command("insert into `Client-Type` set foo = 'bar'").close();
      db.commit();

      ResultSet result = db.query("Select from `Client-Type`");
      Assert.assertEquals(result.stream().count(), 1);
    } catch (CommandSQLParsingException ex) {
      Assert.fail();
    }
  }
}
