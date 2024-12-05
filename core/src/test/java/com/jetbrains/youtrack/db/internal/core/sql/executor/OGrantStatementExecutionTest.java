package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityPolicyImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityRole;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OGrantStatementExecutionTest extends DBTestBase {

  @Test
  public void testSimple() {
    db.begin();
    ORole testRole =
        db.getMetadata()
            .getSecurity()
            .createRole("testRole", OSecurityRole.ALLOW_MODES.DENY_ALL_BUT);
    Assert.assertFalse(
        testRole.allow(ORule.ResourceGeneric.SERVER, "server", ORole.PERMISSION_EXECUTE));
    db.commit();
    db.begin();
    db.command("GRANT execute on server.remove to testRole");
    db.commit();
    testRole = db.getMetadata().getSecurity().getRole("testRole");
    Assert.assertTrue(
        testRole.allow(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE));
  }

  @Test
  public void testGrantPolicy() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    db.command("GRANT POLICY testPolicy ON database.class.Person TO reader").close();
    db.commit();

    Assert.assertEquals(
        "testPolicy",
        security
            .getSecurityPolicies(db, security.getRole(db, "reader"))
            .get("database.class.Person")
            .getName(db));
  }
}
