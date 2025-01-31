package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityPolicy;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AlterSecurityPolicyStatementExecutionTest extends DbTestBase {

  @Test
  public void testPlain() {
    db.begin();
    db.command("CREATE SECURITY POLICY foo").close();
    db.command("ALTER SECURITY POLICY foo SET READ = (name = 'foo')").close();
    db.commit();

    var security = db.getSharedContext().getSecurity();
    SecurityPolicy policy = security.getSecurityPolicy(db, "foo");
    Assert.assertNotNull(policy);
    Assert.assertNotNull("foo", policy.getName(db));
    Assert.assertEquals("name = 'foo'", policy.getReadRule(db));
    Assert.assertNull(policy.getCreateRule(db));
    Assert.assertNull(policy.getBeforeUpdateRule(db));
    Assert.assertNull(policy.getAfterUpdateRule(db));
    Assert.assertNull(policy.getDeleteRule(db));
    Assert.assertNull(policy.getExecuteRule(db));

    db.begin();
    db.command("ALTER SECURITY POLICY foo REMOVE READ").close();
    db.commit();

    policy = security.getSecurityPolicy(db, "foo");
    Assert.assertNotNull(policy);
    Assert.assertNull(policy.getReadRule(db));
  }
}
