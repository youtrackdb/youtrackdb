package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.metadata.security.OSecurityInternal;
import com.orientechnologies.core.metadata.security.OSecurityPolicy;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OAlterSecurityPolicyStatementExecutionTest extends DBTestBase {

  @Test
  public void testPlain() {
    db.begin();
    db.command("CREATE SECURITY POLICY foo").close();
    db.command("ALTER SECURITY POLICY foo SET READ = (name = 'foo')").close();
    db.commit();

    OSecurityInternal security = db.getSharedContext().getSecurity();
    OSecurityPolicy policy = security.getSecurityPolicy(db, "foo");
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
