package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CreateUserStatementExecutionTest extends DbTestBase {

  @Test
  public void testPlain() {
    String name = "testPlain";
    db.begin();
    ResultSet result = db.command("CREATE USER test IDENTIFIED BY foo ROLE admin");
    db.commit();
    result.close();

    result = db.query("SELECT name, roles.name as roles FROM OUser WHERE name = 'test'");
    Assert.assertTrue(result.hasNext());
    Result user = result.next();
    Assert.assertEquals("test", user.getProperty("name"));
    List<String> roles = user.getProperty("roles");
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals("admin", roles.get(0));

    result.close();
  }
}
