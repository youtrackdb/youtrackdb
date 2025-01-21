package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityPolicyImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class RevokeStatementExecutionTest {

  static YouTrackDB youTrackDB;
  private DatabaseSessionInternal db;

  @BeforeClass
  public static void beforeClass() {
    youTrackDB = new YouTrackDBImpl("plocal:.", YouTrackDBConfig.defaultConfig());
  }

  @AfterClass
  public static void afterClass() {
    youTrackDB.close();
  }

  @Before
  public void before() {
    CreateDatabaseUtil.createDatabase("test", youTrackDB, CreateDatabaseUtil.TYPE_MEMORY);
    this.db = (DatabaseSessionInternal) youTrackDB.open("test", "admin",
        CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    youTrackDB.drop("test");
    this.db = null;
  }

  @Test
  public void testSimple() {
    db.begin();
    Role testRole =
        db.getMetadata()
            .getSecurity()
            .createRole("testRole");
    Assert.assertFalse(
        testRole.allow(Rule.ResourceGeneric.SERVER, "server", Role.PERMISSION_EXECUTE));
    db.commit();

    db.begin();
    db.command("GRANT execute on server.remove to testRole");
    db.commit();
    testRole = db.getMetadata().getSecurity().getRole("testRole");
    Assert.assertTrue(
        testRole.allow(Rule.ResourceGeneric.SERVER, "remove", Role.PERMISSION_EXECUTE));
    db.begin();
    db.command("REVOKE execute on server.remove from testRole");
    db.commit();
    testRole = db.getMetadata().getSecurity().getRole("testRole");
    Assert.assertFalse(
        testRole.allow(Rule.ResourceGeneric.SERVER, "remove", Role.PERMISSION_EXECUTE));
  }

  @Test
  public void testRemovePolicy() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
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
