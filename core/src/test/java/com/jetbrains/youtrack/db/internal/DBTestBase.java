package com.jetbrains.youtrack.db.internal;

import com.jetbrains.youtrack.db.internal.core.db.ODatabasePool;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseType;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilder;
import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public class DBTestBase {

  private static final AtomicLong counter = new AtomicLong();
  private static final ConcurrentHashMap<Class<?>, Long> ids = new ConcurrentHashMap<>();

  protected YTDatabaseSessionInternal db;
  protected ODatabasePool pool;
  protected YouTrackDB context;
  @Rule
  public TestName name = new TestName();
  protected String databaseName;
  protected ODatabaseType dbType;

  protected String user = "admin";
  protected String password = "adminpwd";


  @Before
  public void beforeTest() throws Exception {
    context = createContext();
    String dbName = name.getMethodName();
    dbName = dbName.replace('[', '_');
    dbName = dbName.replace(']', '_');
    this.databaseName = dbName;

    createDatabase(dbType);

  }

  public void createDatabase() {
    createDatabase(dbType);
  }

  protected void createDatabase(ODatabaseType dbType) {
    if (db != null && !db.isClosed()) {
      db.close();
    }
    if (pool != null && !pool.isClosed()) {
      pool.close();
    }

    context.create(this.databaseName, dbType, user, password, "admin");
    pool = context.cachedPool(this.databaseName, user, password);
    db = (YTDatabaseSessionInternal) context.open(this.databaseName, "admin", "adminpwd");
  }

  public static String embeddedDBUrl(Class<?> testClass) {
    return "embedded:" + getDirectoryPath(testClass);
  }

  public static String getDirectoryPath(Class<?> testClass) {
    final String buildDirectory = Path.of(System.getProperty("buildDirectory", "./target"))
        .toAbsolutePath().toString();
    return
        buildDirectory + File.separator + "databases" + File.separator + testClass
            .getSimpleName() + "-" + getTestId(testClass);
  }

  private static long getTestId(Class<?> testClass) {
    return ids.computeIfAbsent(testClass, k -> counter.incrementAndGet());
  }

  protected YouTrackDB createContext() {
    var directoryPath = getDirectoryPath(getClass());
    var builder = YouTrackDBConfig.builder();
    var config = createConfig(builder);

    final String testConfig =
        System.getProperty("youtrackdb.test.env", ODatabaseType.MEMORY.name().toLowerCase());

    if ("ci".equals(testConfig) || "release".equals(testConfig)) {
      dbType = ODatabaseType.PLOCAL;
    } else {
      dbType = ODatabaseType.MEMORY;
    }

    return YouTrackDB.embedded(directoryPath, config);
  }

  @SuppressWarnings("SameParameterValue")
  protected void reOpen(String user, String password) {
    if (!pool.isClosed()) {
      pool.close();
      this.pool = context.cachedPool(this.databaseName, user, password);
    }

    if (!db.isClosed()) {
      db.activateOnCurrentThread();
      db.close();
      this.db = (YTDatabaseSessionInternal) context.open(this.databaseName, user, password);
    }
  }

  public YTDatabaseSessionInternal openDatabase() {
    return (YTDatabaseSessionInternal) context.open(this.databaseName, user, password);
  }

  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    return builder.build();
  }

  @After
  public void afterTest() {
    dropDatabase();
    context.close();
  }

  public void dropDatabase() {
    if (!db.isClosed()) {
      db.activateOnCurrentThread();
      db.close();
    }
    if (!pool.isClosed()) {
      pool.close();
    }

    if (context.exists(this.databaseName)) {
      context.drop(databaseName);
    }
  }

  public static void assertWithTimeout(YTDatabaseSession session, Runnable runnable)
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
