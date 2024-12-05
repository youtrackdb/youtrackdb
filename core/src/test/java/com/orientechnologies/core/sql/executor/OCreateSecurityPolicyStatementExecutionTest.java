package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.metadata.security.OSecurityInternal;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCreateSecurityPolicyStatementExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {
    db.begin();
    YTResultSet result = db.command("CREATE SECURITY POLICY foo");
    result.close();
    db.commit();

    db.begin();
    OSecurityInternal security = db.getSharedContext().getSecurity();
    Assert.assertNotNull(security.getSecurityPolicy(db, "foo"));
    db.commit();
  }
}
