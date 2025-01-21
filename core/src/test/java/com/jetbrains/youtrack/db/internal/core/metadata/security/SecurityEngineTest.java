package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBooleanExpression;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SecurityEngineTest {

  static YouTrackDB youTrackDB;
  private DatabaseSessionInternal db;
  private static final String DB_NAME = "test";

  @BeforeClass
  public static void beforeClass() {
    youTrackDB =
        new YouTrackDBImpl(
            "plocal:./target/securityEngineTest",
            YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
  }

  @AfterClass
  public static void afterClass() {
    youTrackDB.close();
  }

  @Before
  public void before() {
    youTrackDB.execute(
        "create database "
            + DB_NAME
            + " "
            + "memory"
            + " users ( admin identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    this.db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    youTrackDB.drop(DB_NAME);
    this.db = null;
  }

  @Test
  public void testAllClasses() {
    SecurityInternal security = db.getSharedContext().getSecurity();
    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'admin'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.*", policy);
    db.commit();

    SQLBooleanExpression pred =
        SecurityEngine.getPredicateForSecurityResource(
            db, (SecurityShared) security, "database.class.Person", SecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'admin'", pred.toString());
  }

  @Test
  public void testSingleClass() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);
    db.commit();

    SQLBooleanExpression pred =
        SecurityEngine.getPredicateForSecurityResource(
            db, (SecurityShared) security, "database.class.Person", SecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'foo'", pred.toString());
  }

  @Test
  public void testSuperclass() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");
    db.createClass("Employee", "Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);
    db.commit();

    SQLBooleanExpression pred =
        SecurityEngine.getPredicateForSecurityResource(
            db, (SecurityShared) security, "database.class.Employee", SecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'foo'", pred.toString());
  }

  @Test
  public void testSuperclass2() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");
    db.createClass("Employee", "Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    policy = security.createSecurityPolicy(db, "policy2");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "admin"), "database.class.Employee", policy);
    db.commit();

    SQLBooleanExpression pred =
        SecurityEngine.getPredicateForSecurityResource(
            db, (SecurityShared) security, "database.class.Employee", SecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'bar'", pred.toString());
  }

  @Test
  public void testSuperclass3() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");
    db.createClass("Employee", "Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'admin'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    policy = security.createSecurityPolicy(db, "policy2");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'bar' OR name = 'admin'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.*", policy);
    db.commit();

    SQLBooleanExpression pred =
        SecurityEngine.getPredicateForSecurityResource(
            db, (SecurityShared) security, "database.class.Employee", SecurityPolicy.Scope.READ);

    Assert.assertEquals("name = 'admin'", pred.toString());
  }

  @Test
  public void testTwoSuperclasses() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");
    db.createClass("Foo");
    db.createClass("Employee", "Person", "Foo");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    policy = security.createSecurityPolicy(db, "policy2");
    policy.setActive(db, true);
    policy.setReadRule(db, "surname = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Foo", policy);
    db.commit();

    SQLBooleanExpression pred =
        SecurityEngine.getPredicateForSecurityResource(
            db, (SecurityShared) security, "database.class.Employee", SecurityPolicy.Scope.READ);

    Assert.assertTrue(
        "name = 'foo' AND surname = 'bar'".equals(pred.toString())
            || "surname = 'bar' AND name = 'foo'".equals(pred.toString()));
  }

  @Test
  public void testTwoRoles() {

    db.begin();
    db.command(
        "Update " + Role.CLASS_NAME + " set roles = roles || (select from " + Role.CLASS_NAME
            + " where name = 'reader') where name ="
            + " 'admin'");
    db.commit();
    db.close();
    db =
        (DatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);

    policy = security.createSecurityPolicy(db, "policy2");
    policy.setActive(db, true);
    policy.setReadRule(db, "surname = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);
    db.commit();

    SQLBooleanExpression pred =
        SecurityEngine.getPredicateForSecurityResource(
            db, (SecurityShared) security, "database.class.Person", SecurityPolicy.Scope.READ);

    Assert.assertTrue(
        "name = 'foo' OR surname = 'bar'".equals(pred.toString())
            || "surname = 'bar' OR name = 'foo'".equals(pred.toString()));
  }

  @Test
  public void testRecordFiltering() {
    SecurityInternal security = db.getSharedContext().getSecurity();

    db.createClass("Person");
    var rec1 =
        db.computeInTx(
            () -> {
              Entity record1 = db.newEntity("Person");
              record1.setProperty("name", "foo");
              record1.save();
              return record1;
            });

    var rec2 =
        db.computeInTx(
            () -> {
              Entity record2 = db.newEntity("Person");
              record2.setProperty("name", "bar");
              record2.save();
              return record2;
            });

    db.begin();
    SecurityPolicyImpl policy = security.createSecurityPolicy(db, "policy1");
    policy.setActive(db, true);
    policy.setReadRule(db, "name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "admin"), "database.class.Person", policy);
    db.commit();

    db.bindToSession(rec1);
    Assert.assertTrue(rec1.getIdentity().isPersistent());

    try {
      db.bindToSession(rec2);
      Assert.fail();
    } catch (RecordNotFoundException e) {
      // ignore
    }
  }
}
