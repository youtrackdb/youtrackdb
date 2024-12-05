package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCreateUserStatementExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {
    String name = "testPlain";
    db.begin();
    YTResultSet result = db.command("CREATE USER test IDENTIFIED BY foo ROLE admin");
    db.commit();
    result.close();

    result = db.query("SELECT name, roles.name as roles FROM OUser WHERE name = 'test'");
    Assert.assertTrue(result.hasNext());
    YTResult user = result.next();
    Assert.assertEquals("test", user.getProperty("name"));
    List<String> roles = user.getProperty("roles");
    Assert.assertEquals(1, roles.size());
    Assert.assertEquals("admin", roles.get(0));

    result.close();
  }
}
