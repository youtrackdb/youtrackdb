package com.orientechnologies;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public class BaseMemoryDatabase {

  protected ODatabaseSessionInternal db;
  protected OrientDB context;
  @Rule public TestName name = new TestName();
  private String databaseName;

  @Before
  public void beforeTest() {
    var builder = OrientDBConfig.builder();
    context = OrientDB.embedded(this.getClass().getSimpleName(), createConfig(builder));
    String dbName = name.getMethodName();
    dbName = dbName.replace('[', '_');
    dbName = dbName.replace(']', '_');
    this.databaseName = dbName;
    context.create(this.databaseName, ODatabaseType.MEMORY, "admin", "adminpwd", "admin");
    db = (ODatabaseSessionInternal) context.open(this.databaseName, "admin", "adminpwd");
  }

  @SuppressWarnings("SameParameterValue")
  protected void reOpen(String user, String password) {
    this.db.close();
    this.db = (ODatabaseSessionInternal) context.open(this.databaseName, user, password);
  }

  protected OrientDBConfig createConfig(OrientDBConfigBuilder builder) {
    return builder.build();
  }

  @After
  public void afterTest() {
    db.close();
    context.drop(databaseName);
    context.close();
  }

  public static void assertWithTimeout(ODatabaseDocument session, Runnable runnable)
      throws Exception {
    for (int i = 0; i < 30 * 60 * 10; i++) {
      try {
        session.begin();
        runnable.run();
        session.commit();
        return;
      } catch (AssertionError e) {
        session.rollback();
        Thread.sleep(100);
      } catch (Exception e) {
        session.rollback();
        throw e;
      }
    }

    runnable.run();
  }
}
