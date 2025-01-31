package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class SecurityPolicyTest extends DbTestBase {

  @Test
  public void testSecurityPolicyCreate() {
    var rs =
        db.query(
            "select from " + SecurityPolicy.CLASS_NAME + " WHERE name = ?", "test");
    Assert.assertFalse(rs.hasNext());
    rs.close();
    var security = db.getSharedContext().getSecurity();

    db.begin();
    security.createSecurityPolicy(db, "test");
    db.commit();

    rs =
        db.query(
            "select from " + SecurityPolicy.CLASS_NAME + " WHERE name = ?", "test");
    Assert.assertTrue(rs.hasNext());
    var item = rs.next();
    Assert.assertEquals("test", item.getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSecurityPolicyGet() {
    var security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    security.createSecurityPolicy(db, "test");
    db.commit();

    Assert.assertNotNull(security.getSecurityPolicy(db, "test"));
  }

  @Test
  public void testValidPredicates() {
    var security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    var policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule(db, "name = 'create'");
    policy.setReadRule(db, "name = 'read'");
    policy.setBeforeUpdateRule(db, "name = 'beforeUpdate'");
    policy.setAfterUpdateRule(db, "name = 'afterUpdate'");
    policy.setDeleteRule(db, "name = 'delete'");
    policy.setExecuteRule(db, "name = 'execute'");

    security.saveSecurityPolicy(db, policy);
    db.commit();

    SecurityPolicy readPolicy = security.getSecurityPolicy(db, "test");
    Assert.assertNotNull(policy);
    Assert.assertEquals("name = 'create'", readPolicy.getCreateRule(db));
    Assert.assertEquals("name = 'read'", readPolicy.getReadRule(db));
    Assert.assertEquals("name = 'beforeUpdate'", readPolicy.getBeforeUpdateRule(db));
    Assert.assertEquals("name = 'afterUpdate'", readPolicy.getAfterUpdateRule(db));
    Assert.assertEquals("name = 'delete'", readPolicy.getDeleteRule(db));
    Assert.assertEquals("name = 'execute'", readPolicy.getExecuteRule(db));
  }

  @Test
  public void testInvalidPredicates() {
    var security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    var policy = security.createSecurityPolicy(db, "test");
    db.commit();
    try {
      db.begin();
      policy.setCreateRule(db, "foo bar");
      db.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      db.begin();
      policy.setReadRule(db, "foo bar");
      db.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      db.begin();
      policy.setBeforeUpdateRule(db, "foo bar");
      db.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      db.begin();
      policy.setAfterUpdateRule(db, "foo bar");
      db.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      db.begin();
      policy.setDeleteRule(db, "foo bar");
      db.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
    try {
      db.begin();
      policy.setExecuteRule(db, "foo bar");
      db.commit();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
    }
  }

  @Test
  public void testAddPolicyToRole() {
    var security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    var policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule(db, "1 = 1");
    policy.setBeforeUpdateRule(db, "1 = 2");
    policy.setActive(db, true);
    security.saveSecurityPolicy(db, policy);
    db.commit();

    db.begin();
    var reader = security.getRole(db, "reader");
    var resource = "database.class.Person";
    security.setSecurityPolicy(db, reader, resource, policy);
    db.commit();

    var policyRid = policy.getElement(db).getIdentity();
    try (var rs = db.query("select from " + Role.CLASS_NAME + " where name = 'reader'")) {
      Map<String, Identifiable> rolePolicies = rs.next().getProperty("policies");
      var id = rolePolicies.get(resource);
      Assert.assertEquals(id.getIdentity(), policyRid);
    }

    var policy2 = security.getSecurityPolicy(db, reader, resource);
    Assert.assertNotNull(policy2);
    Assert.assertEquals(policy2.getIdentity(), policyRid);
  }

  @Test
  public void testRemovePolicyToRole() {
    var security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    var policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule(db, "1 = 1");
    policy.setBeforeUpdateRule(db, "1 = 2");
    policy.setActive(db, true);
    security.saveSecurityPolicy(db, policy);
    db.commit();

    db.begin();
    var reader = security.getRole(db, "reader");
    var resource = "database.class.Person";
    security.setSecurityPolicy(db, reader, resource, policy);

    security.removeSecurityPolicy(db, reader, resource);
    db.commit();

    Assert.assertNull(security.getSecurityPolicy(db, reader, resource));
  }
}
