package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityInternal;
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
