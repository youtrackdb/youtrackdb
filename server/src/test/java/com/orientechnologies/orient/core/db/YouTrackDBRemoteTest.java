package com.orientechnologies.orient.core.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.db.ODatabasePool;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseType;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.exception.YTStorageException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class YouTrackDBRemoteTest {

  private static final String SERVER_DIRECTORY = "./target/dbfactory";
  private OServer server;

  private YouTrackDB factory;

  @Before
  public void before() throws Exception {
    ODatabaseRecordThreadLocal.instance().remove();

    GlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    server.activate();

    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(GlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT, 300_000)
            .build();

    factory = new YouTrackDB("remote:localhost", "root", "root", config);
  }

  @Test
  public void createAndUseRemoteDatabase() {
    if (!factory.exists("test")) {
      factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    }

    YTDatabaseSessionInternal db = (YTDatabaseSessionInternal) factory.open("test", "admin",
        "admin");
    db.begin();
    db.save(new EntityImpl(), db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();
    db.close();
  }

  // @Test(expected = YTStorageExistsException.class)
  // TODO: Uniform database exist exceptions
  @Test(expected = YTStorageException.class)
  public void doubleCreateRemoteDatabase() {
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
  }

  @Test
  public void createDropRemoteDatabase() {
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    assertTrue(factory.exists("test"));
    factory.drop("test");
    assertFalse(factory.exists("test"));
  }

  @Test
  public void testPool() {
    if (!factory.exists("test")) {
      factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    }

    ODatabasePool pool = new ODatabasePool(factory, "test", "admin", "admin");
    YTDatabaseSessionInternal db = (YTDatabaseSessionInternal) pool.acquire();
    db.begin();
    db.save(new EntityImpl(), db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();
    db.close();
    pool.close();
  }

  @Test
  @Ignore
  public void testCachedPool() {
    if (!factory.exists("testdb")) {
      factory.execute(
          "create database testdb memory users (admin identified by 'admin' role admin, reader"
              + " identified by 'reader' role reader, writer identified by 'writer' role writer)");
    }

    ODatabasePool poolAdmin1 = factory.cachedPool("testdb", "admin", "admin");
    ODatabasePool poolAdmin2 = factory.cachedPool("testdb", "admin", "admin");
    ODatabasePool poolReader1 = factory.cachedPool("testdb", "reader", "reader");
    ODatabasePool poolReader2 = factory.cachedPool("testdb", "reader", "reader");

    assertEquals(poolAdmin1, poolAdmin2);
    assertEquals(poolReader1, poolReader2);
    assertNotEquals(poolAdmin1, poolReader1);

    ODatabasePool poolWriter1 = factory.cachedPool("testdb", "writer", "writer");
    ODatabasePool poolWriter2 = factory.cachedPool("testdb", "writer", "writer");
    assertEquals(poolWriter1, poolWriter2);

    ODatabasePool poolAdmin3 = factory.cachedPool("testdb", "admin", "admin");
    assertNotEquals(poolAdmin1, poolAdmin3);

    poolAdmin1.close();
    poolReader1.close();
    poolWriter1.close();
  }

  @Test
  public void testCachedPoolFactoryCleanUp() throws Exception {
    if (!factory.exists("testdb")) {
      factory.execute(
          "create database testdb memory users (admin identified by 'admin' role admin)");
    }

    ODatabasePool poolAdmin1 = factory.cachedPool("testdb", "admin", "admin");
    ODatabasePool poolAdmin2 = factory.cachedPool("testdb", "admin", "admin");

    assertFalse(poolAdmin1.isClosed());
    assertEquals(poolAdmin1, poolAdmin2);

    poolAdmin1.close();

    assertTrue(poolAdmin1.isClosed());

    Thread.sleep(5_000);

    ODatabasePool poolAdmin3 = factory.cachedPool("testdb", "admin", "admin");
    assertNotEquals(poolAdmin1, poolAdmin3);
    assertFalse(poolAdmin3.isClosed());

    poolAdmin3.close();
  }

  @Test
  @Ignore
  public void testMultiThread() {
    if (!factory.exists("test")) {
      factory.execute(
          "create database test memory users (admin identified by 'admin' role admin, reader"
              + " identified by 'reader' role reader, writer identified by 'writer' role writer)");
    }

    ODatabasePool pool = new ODatabasePool(factory, "test", "admin", "admin");

    // do a query and assert on other thread
    Runnable acquirer =
        () -> {
          var db = pool.acquire();

          try {
            assertThat(db.isActiveOnCurrentThread()).isTrue();

            YTResultSet res = db.query("SELECT * FROM OUser");

            assertEquals(3, res.stream().count());

          } finally {

            db.close();
          }
        };

    // spawn 20 threads
    List<CompletableFuture<Void>> futures =
        IntStream.range(0, 19)
            .boxed()
            .map(i -> CompletableFuture.runAsync(acquirer))
            .collect(Collectors.toList());

    futures.forEach(cf -> cf.join());

    pool.close();
  }

  @Test
  public void testListDatabases() {
    assertEquals(0, factory.list().size());
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    List<String> databases = factory.list();
    assertEquals(1, databases.size());
    assertTrue(databases.contains("test"));
  }

  @Test
  public void createDatabaseNoUsers() {
    factory.create(
        "noUser",
        ODatabaseType.MEMORY,
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build());
    try (YTDatabaseSession session = factory.open("noUser", "root", "root")) {
      assertEquals(0, session.query("select from OUser").stream().count());
    }
  }

  @Test
  public void createDatabaseDefaultUsers() {
    factory.create(
        "noUser",
        ODatabaseType.MEMORY,
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, true)
            .build());
    try (YTDatabaseSession session = factory.open("noUser", "root", "root")) {
      assertEquals(3, session.query("select from OUser").stream().count());
    }
  }

  @Test
  public void testCopyOpenedDatabase() {
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    YTDatabaseSession db1;
    try (YTDatabaseSessionInternal db =
        (YTDatabaseSessionInternal) factory.open("test", "admin", "admin")) {
      db1 = db.copy();
    }
    db1.activateOnCurrentThread();
    assertFalse(db1.isClosed());
    db1.close();
  }

  @Test
  public void testCreateDatabaseViaSQL() {
    String dbName = "testCreateDatabaseViaSQL";

    try (YTResultSet result = factory.execute("create database " + dbName + " plocal")) {
      Assert.assertTrue(result.hasNext());
      YTResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(factory.exists(dbName));
    factory.drop(dbName);
  }

  @After
  public void after() {
    for (String db : factory.list()) {
      factory.drop(db);
    }

    factory.close();
    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBManager.instance().startup();

    ODatabaseRecordThreadLocal.instance().remove();
  }
}
