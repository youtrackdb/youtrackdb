package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class OSecurityPolicyTest extends DBTestBase {

  @Test
  public void testSecurityPolicyCreate() {
    OResultSet rs =
        db.query(
            "select from " + OSecurityPolicy.class.getSimpleName() + " WHERE name = ?", "test");
    Assert.assertFalse(rs.hasNext());
    rs.close();
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.begin();
    security.createSecurityPolicy(db, "test");
    db.commit();

    rs =
        db.query(
            "select from " + OSecurityPolicy.class.getSimpleName() + " WHERE name = ?", "test");
    Assert.assertTrue(rs.hasNext());
    OResult item = rs.next();
    Assert.assertEquals("test", item.getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSecurityPolicyGet() {
    OSecurityInternal security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    security.createSecurityPolicy(db, "test");
    db.commit();

    Assert.assertNotNull(security.getSecurityPolicy(db, "test"));
  }

  @Test
  public void testValidPredicates() {
    OSecurityInternal security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule(db, "name = 'create'");
    policy.setReadRule(db, "name = 'read'");
    policy.setBeforeUpdateRule(db, "name = 'beforeUpdate'");
    policy.setAfterUpdateRule(db, "name = 'afterUpdate'");
    policy.setDeleteRule(db, "name = 'delete'");
    policy.setExecuteRule(db, "name = 'execute'");

    security.saveSecurityPolicy(db, policy);
    db.commit();

    OSecurityPolicy readPolicy = security.getSecurityPolicy(db, "test");
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
    OSecurityInternal security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "test");
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
    OSecurityInternal security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule(db, "1 = 1");
    policy.setBeforeUpdateRule(db, "1 = 2");
    policy.setActive(db, true);
    security.saveSecurityPolicy(db, policy);
    db.commit();

    db.begin();
    ORole reader = security.getRole(db, "reader");
    String resource = "database.class.Person";
    security.setSecurityPolicy(db, reader, resource, policy);
    db.commit();

    YTRID policyRid = policy.getElement(db).getIdentity();
    try (OResultSet rs = db.query("select from ORole where name = 'reader'")) {
      Map<String, YTIdentifiable> rolePolicies = rs.next().getProperty("policies");
      YTIdentifiable id = rolePolicies.get(resource);
      Assert.assertEquals(id.getIdentity(), policyRid);
    }

    OSecurityPolicy policy2 = security.getSecurityPolicy(db, reader, resource);
    Assert.assertNotNull(policy2);
    Assert.assertEquals(policy2.getIdentity(), policyRid);
  }

  @Test
  public void testRemovePolicyToRole() {
    OSecurityInternal security = db.getSharedContext().getSecurity();
    Assert.assertNull(security.getSecurityPolicy(db, "test"));

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "test");
    policy.setCreateRule(db, "1 = 1");
    policy.setBeforeUpdateRule(db, "1 = 2");
    policy.setActive(db, true);
    security.saveSecurityPolicy(db, policy);
    db.commit();

    db.begin();
    ORole reader = security.getRole(db, "reader");
    String resource = "database.class.Person";
    security.setSecurityPolicy(db, reader, resource, policy);

    security.removeSecurityPolicy(db, reader, resource);
    db.commit();

    Assert.assertNull(security.getSecurityPolicy(db, reader, resource));
  }
}
