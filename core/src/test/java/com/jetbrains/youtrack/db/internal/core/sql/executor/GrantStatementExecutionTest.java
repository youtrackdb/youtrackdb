package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityPolicyImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityRole;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class GrantStatementExecutionTest extends DbTestBase {

  @Test
  public void testSimple() {
    db.begin();
    Role testRole =
        db.getMetadata()
            .getSecurity()
            .createRole("testRole", SecurityRole.ALLOW_MODES.DENY_ALL_BUT);
    Assert.assertFalse(
        testRole.allow(Rule.ResourceGeneric.SERVER, "server", Role.PERMISSION_EXECUTE));
    db.commit();
    db.begin();
    db.command("GRANT execute on server.remove to testRole");
    db.commit();
    testRole = db.getMetadata().getSecurity().getRole("testRole");
    Assert.assertTrue(
        testRole.allow(Rule.ResourceGeneric.SERVER, "remove", Role.PERMISSION_EXECUTE));
  }

  @Test
  public void testGrantPolicy() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
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
