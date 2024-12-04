package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicyImpl;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class ORevokeStatementExecutionTest {

  static YouTrackDB orient;
  private ODatabaseSessionInternal db;

  @BeforeClass
  public static void beforeClass() {
    orient = new YouTrackDB("plocal:.", YouTrackDBConfig.defaultConfig());
  }

  @AfterClass
  public static void afterClass() {
    orient.close();
  }

  @Before
  public void before() {
    OCreateDatabaseUtil.createDatabase("test", orient, OCreateDatabaseUtil.TYPE_MEMORY);
    this.db = (ODatabaseSessionInternal) orient.open("test", "admin",
        OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    orient.drop("test");
    this.db = null;
  }

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
    db.begin();
    db.command("REVOKE execute on server.remove from testRole");
    db.commit();
    testRole = db.getMetadata().getSecurity().getRole("testRole");
    Assert.assertFalse(
        testRole.allow(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE));
  }

  @Test
  public void testRemovePolicy() {
    OSecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
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
    db.begin();
    db.command("REVOKE POLICY ON database.class.Person FROM reader").close();
    db.commit();

    Assert.assertNull(
        security
            .getSecurityPolicies(db, security.getRole(db, "reader"))
            .get("database.class.Person"));
  }
}
