package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

public class SecuritySharedTest extends DbTestBase {

  @Test
  public void testCreateSecurityPolicy() {
    var security = db.getSharedContext().getSecurity();
    db.begin();
    security.createSecurityPolicy(db, "testPolicy");
    db.commit();
    Assert.assertNotNull(security.getSecurityPolicy(db, "testPolicy"));
  }

  @Test
  public void testDeleteSecurityPolicy() {
    var security = db.getSharedContext().getSecurity();
    db.begin();
    security.createSecurityPolicy(db, "testPolicy");
    db.commit();

    db.begin();
    security.deleteSecurityPolicy(db, "testPolicy");
    db.commit();

    Assert.assertNull(security.getSecurityPolicy(db, "testPolicy"));
  }

  @Test
  public void testUpdateSecurityPolicy() {
    var security = db.getSharedContext().getSecurity();
    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    db.commit();

    Assert.assertTrue(security.getSecurityPolicy(db, "testPolicy").isActive(db));
    Assert.assertEquals("name = 'foo'",
        security.getSecurityPolicy(db, "testPolicy").getReadRule(db));
  }

  @Test
  public void testBindPolicyToRole() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    Assert.assertEquals(
        "testPolicy",
        security
            .getSecurityPolicies(db, security.getRole(db, "reader"))
            .get("database.class.Person")
            .getName(db));
  }

  @Test
  public void testUnbindPolicyFromRole() {
    var security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    var policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    db.begin();
    security.removeSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person");
    db.commit();

    Assert.assertNull(
        security
            .getSecurityPolicies(db, security.getRole(db, "reader"))
            .get("database.class.Person"));
  }
}
