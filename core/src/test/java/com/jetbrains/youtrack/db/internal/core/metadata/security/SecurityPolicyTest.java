package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class SecurityPolicyTest extends DbTestBase {

  @Test
  public void testSecurityPolicyCreate() {
    var rs =
        session.query(
            "select from " + SecurityPolicy.CLASS_NAME + " WHERE name = ?", "test");
    Assert.assertFalse(rs.hasNext());
    rs.close();
    var security = session.getSharedContext().getSecurity();

    session.begin();
    security.createSecurityPolicy(session, "test");
    session.commit();

    rs =
        session.query(
            "select from " + SecurityPolicy.CLASS_NAME + " WHERE name = ?", "test");
    Assert.assertTrue(rs.hasNext());
    var item = rs.next();
    Assert.assertEquals("test", item.getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSecurityPolicyGet() {
    var security = session.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(session, "test"));

    session.begin();
    security.createSecurityPolicy(session, "test");
    session.commit();

    Assert.assertNotNull(security.getSecurityPolicy(session, "test"));
  }

  @Test
  public void testValidPredicates() {
    var security = session.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(session, "test"));

    session.begin();
    var policy = security.createSecurityPolicy(session, "test");
    policy.setCreateRule(session, "name = 'create'");
    policy.setReadRule(session, "name = 'read'");
    policy.setBeforeUpdateRule(session, "name = 'beforeUpdate'");
    policy.setAfterUpdateRule(session, "name = 'afterUpdate'");
    policy.setDeleteRule(session, "name = 'delete'");
    policy.setExecuteRule(session, "name = 'execute'");

    security.saveSecurityPolicy(session, policy);
    session.commit();

    SecurityPolicy readPolicy = security.getSecurityPolicy(session, "test");
    Assert.assertNotNull(policy);
    Assert.assertEquals("name = 'create'", readPolicy.getCreateRule(session));
    Assert.assertEquals("name = 'read'", readPolicy.getReadRule(session));
    Assert.assertEquals("name = 'beforeUpdate'", readPolicy.getBeforeUpdateRule(session));
    Assert.assertEquals("name = 'afterUpdate'", readPolicy.getAfterUpdateRule(session));
    Assert.assertEquals("name = 'delete'", readPolicy.getDeleteRule(session));
    Assert.assertEquals("name = 'execute'", readPolicy.getExecuteRule(session));
  }

  @Test
  public void testInvalidPredicates() {
    var security = session.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(session, "test"));

    session.begin();
    var policy = security.createSecurityPolicy(session, "test");
    session.commit();
    try {
      session.begin();
      policy.setCreateRule(session, "foo bar");
      session.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      session.begin();
      policy.setReadRule(session, "foo bar");
      session.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      session.begin();
      policy.setBeforeUpdateRule(session, "foo bar");
      session.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      session.begin();
      policy.setAfterUpdateRule(session, "foo bar");
      session.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      session.begin();
      policy.setDeleteRule(session, "foo bar");
      session.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      session.begin();
      policy.setExecuteRule(session, "foo bar");
      session.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
  }

  @Test
  public void testAddPolicyToRole() {
    var security = session.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(session, "test"));

    session.begin();
    var policy = security.createSecurityPolicy(session, "test");
    policy.setCreateRule(session, "1 = 1");
    policy.setBeforeUpdateRule(session, "1 = 2");
    policy.setActive(session, true);
    security.saveSecurityPolicy(session, policy);
    session.commit();

    session.begin();
    var reader = security.getRole(session, "reader");
    var resource = "database.class.Person";
    security.setSecurityPolicy(session, reader, resource, policy);
    session.commit();

    var policyRid = policy.getEntity(session).getIdentity();
    try (var rs = session.query("select from " + Role.CLASS_NAME + " where name = 'reader'")) {
      Map<String, Identifiable> rolePolicies = rs.next().getProperty("policies");
      var id = rolePolicies.get(resource);
      Assert.assertEquals(id.getIdentity(), policyRid);
    }

    var policy2 = security.getSecurityPolicy(session, reader, resource);
    Assert.assertNotNull(policy2);
    Assert.assertEquals(policy2.getIdentity(), policyRid);
  }

  @Test
  public void testRemovePolicyToRole() {
    var security = session.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(session, "test"));

    session.begin();
    var policy = security.createSecurityPolicy(session, "test");
    policy.setCreateRule(session, "1 = 1");
    policy.setBeforeUpdateRule(session, "1 = 2");
    policy.setActive(session, true);
    security.saveSecurityPolicy(session, policy);
    session.commit();

    session.begin();
    var reader = security.getRole(session, "reader");
    var resource = "database.class.Person";
    security.setSecurityPolicy(session, reader, resource, policy);
    security.removeSecurityPolicy(session, reader, resource);
    session.commit();

    Assert.assertNull(security.getSecurityPolicy(session, reader, resource));
  }
}
