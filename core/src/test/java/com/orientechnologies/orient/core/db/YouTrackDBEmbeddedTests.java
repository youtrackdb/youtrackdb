package com.orientechnologies.orient.core.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageDoesNotExistException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.io.File;
import java.util.List;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class YouTrackDBEmbeddedTests {

  @Test
  public void testCompatibleUrl() {
    try (YouTrackDB youTrackDb = new YouTrackDB(
        "plocal:" + DBTestBase.getDirectoryPath(getClass()) + "compatibleUrl",
        YouTrackDBConfig.defaultConfig())) {
    }
    try (YouTrackDB youTrackDb = new YouTrackDB(
        "memory:" + DBTestBase.getDirectoryPath(getClass()) + "compatibleUrl",
        YouTrackDBConfig.defaultConfig())) {
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongUrlFalure() {
    try (YouTrackDB wrong = new YouTrackDB("wrong", YouTrackDBConfig.defaultConfig())) {
    }
  }

  @Test
  public void createAndUseEmbeddedDatabase() {
    try (final YouTrackDB youTrackDb =
        OCreateDatabaseUtil.createDatabase(
            "createAndUseEmbeddedDatabase", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY)) {
      final var db =
          (ODatabaseSessionInternal)
              youTrackDb.open(
                  "createAndUseEmbeddedDatabase", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      db.executeInTx(
          () -> db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId())));
      db.close();
    }
  }

  @Test(expected = ODatabaseException.class)
  public void testEmbeddedDoubleCreate() {
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    try {
      youTrackDb.create("test", ODatabaseType.MEMORY);
      youTrackDb.create("test", ODatabaseType.MEMORY);
    } finally {
      youTrackDb.close();
    }
  }

  @Test
  public void createDropEmbeddedDatabase() {
    YouTrackDB youTrackDb = new YouTrackDB(
        DBTestBase.embeddedDBUrl(getClass()) + "createDropEmbeddedDatabase",
        YouTrackDBConfig.defaultConfig());
    try {
      youTrackDb.create("test", ODatabaseType.MEMORY);
      assertTrue(youTrackDb.exists("test"));
      youTrackDb.drop("test");
      assertFalse(youTrackDb.exists("test"));
    } finally {
      youTrackDb.close();
    }
  }

  @Test
  public void testMultiThread() {
    try (final YouTrackDB youTrackDb =
        OCreateDatabaseUtil.createDatabase(
            "testMultiThread", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY)) {
      final ODatabasePool pool =
          new ODatabasePool(
              youTrackDb, "testMultiThread", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      // do a query and assert on other thread
      Runnable acquirer =
          () -> {
            ODatabaseSession db = pool.acquire();
            try {
              assertThat(db.isActiveOnCurrentThread()).isTrue();
              final OResultSet res = db.query("SELECT * FROM OUser");
              assertThat(res).hasSize(1); // Only 'admin' created in this test
            } finally {
              db.close();
            }
          };

      // spawn 20 threads
      final List<CompletableFuture<Void>> futures =
          IntStream.range(0, 19)
              .boxed()
              .map(i -> CompletableFuture.runAsync(acquirer))
              .toList();

      futures.forEach(CompletableFuture::join);

      pool.close();
    }
  }

  @Test
  public void testListDatabases() {
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()) + "listTest1",
        YouTrackDBConfig.defaultConfig());
    assertEquals(0, youTrackDb.list().size());
    youTrackDb.create("test", ODatabaseType.MEMORY);
    List<String> databases = youTrackDb.list();
    assertEquals(1, databases.size());
    assertTrue(databases.contains("test"));
    youTrackDb.close();
  }

  @Test
  public void testListDatabasesPersistent() {
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()) + "listTest2",
        YouTrackDBConfig.defaultConfig());
    assertEquals(0, youTrackDb.list().size());
    youTrackDb.create("testListDatabase", ODatabaseType.PLOCAL);
    List<String> databases = youTrackDb.list();
    assertEquals(1, databases.size());
    assertTrue(databases.contains("testListDatabase"));
    youTrackDb.drop("testListDatabase");
    youTrackDb.close();
  }

  @Test
  public void testRegisterDatabase() {
    final YouTrackDB youtrack = new YouTrackDB(
        DBTestBase.embeddedDBUrl(getClass()) + "testRegisterDatabase",
        YouTrackDBConfig.defaultConfig());
    try {
      youtrack.execute("create system user admin identified by 'admin' role root");

      final YouTrackDBEmbedded youtrackEmbedded = (YouTrackDBEmbedded) youtrack.getInternal();
      assertEquals(0, youtrackEmbedded.listDatabases("", "").size());
      youtrackEmbedded.initCustomStorage("database1", DBTestBase.getDirectoryPath(getClass()) +
              "testRegisterDatabase/database1",
          "", "");
      try (final ODatabaseSession db = youtrackEmbedded.open("database1", "admin", "admin")) {
        assertEquals("database1", db.getName());
      }
      youtrackEmbedded.initCustomStorage("database2", DBTestBase.getDirectoryPath(getClass()) +
              "testRegisterDatabase/database2",
          "", "");

      try (final ODatabaseSession db = youtrackEmbedded.open("database2", "admin", "admin")) {
        assertEquals("database2", db.getName());
      }
      youtrackEmbedded.drop("database1", null, null);
      youtrackEmbedded.drop("database2", null, null);
      youtrackEmbedded.close();
    } finally {
      OFileUtils.deleteRecursively(
          new File(DBTestBase.getDirectoryPath(getClass()) + "testRegisterDatabase"));
    }
  }

  @Test
  public void testCopyOpenedDatabase() {
    try (final YouTrackDB youTrackDb =
        OCreateDatabaseUtil.createDatabase(
            "testCopyOpenedDatabase", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY)) {
      ODatabaseSession db1;
      try (ODatabaseSessionInternal db =
          (ODatabaseSessionInternal)
              youTrackDb.open(
                  "testCopyOpenedDatabase", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        db1 = db.copy();
      }
      db1.activateOnCurrentThread();
      assertFalse(db1.isClosed());
      db1.close();
    }
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseCreate() {
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.create("test", ODatabaseType.MEMORY);
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseOpen() {
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.open("testUseAfterCloseOpen", "", "");
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseList() {
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.list();
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseExists() {
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.exists("");
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseOpenPoolInternal() {
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.openPool("", "", "", YouTrackDBConfig.defaultConfig());
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseDrop() {
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.drop("");
  }

  @Test
  public void testPoolByUrl() {
    final YouTrackDB youTrackDb =
        OCreateDatabaseUtil.createDatabase(
            "some", DBTestBase.embeddedDBUrl(getClass()) + "poolTest",
            OCreateDatabaseUtil.TYPE_PLOCAL);
    youTrackDb.close();

    final ODatabasePool pool =
        new ODatabasePool(
            DBTestBase.embeddedDBUrl(getClass()) + "poolTest/some", "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    pool.close();
  }

  @Test
  public void testDropTL() {
    final YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    if (!youTrackDb.exists("some")) {
      youTrackDb.execute(
          "create database "
              + "some"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    if (!youTrackDb.exists("some1")) {
      youTrackDb.execute(
          "create database "
              + "some1"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    var db = youTrackDb.open("some", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    youTrackDb.drop("some1");
    db.close();
    youTrackDb.close();
  }

  @Test
  public void testClosePool() {
    try (var youTrackDB = YouTrackDB.embedded(
        DBTestBase.getDirectoryPath(getClass()) + "testClosePool",
        YouTrackDBConfig.defaultConfig())) {
      if (!youTrackDB.exists("test")) {
        youTrackDB.create("test", ODatabaseType.PLOCAL, "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD, "admin");
      }
    }

    final ODatabasePool pool =
        new ODatabasePool(
            DBTestBase.embeddedDBUrl(getClass()) + "testClosePool/test",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertFalse(pool.isClosed());
    pool.close();
    assertTrue(pool.isClosed());

  }

  @Test
  public void testPoolFactory() {
    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    YouTrackDB youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    if (!youTrackDB.exists("testdb")) {
      youTrackDB.execute(
          "create database "
              + "testdb"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin, reader identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role reader, writer identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role writer)");
    }
    ODatabasePool poolAdmin1 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolAdmin2 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolReader1 =
        youTrackDB.cachedPool("testdb", "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabasePool poolReader2 =
        youTrackDB.cachedPool("testdb", "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertEquals(poolAdmin1, poolAdmin2);
    assertEquals(poolReader1, poolReader2);
    assertNotEquals(poolAdmin1, poolReader1);

    ODatabasePool poolWriter1 =
        youTrackDB.cachedPool("testdb", "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabasePool poolWriter2 =
        youTrackDB.cachedPool("testdb", "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertEquals(poolWriter1, poolWriter2);

    ODatabasePool poolAdmin3 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolAdmin1, poolAdmin3);

    youTrackDB.close();
  }

  @Test
  public void testPoolFactoryCleanUp() throws Exception {
    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT, 1_000)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    YouTrackDB youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    if (!youTrackDB.exists("testdb")) {
      youTrackDB.execute(
          "create database "
              + "testdb"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    if (!youTrackDB.exists("testdb1")) {
      youTrackDB.execute(
          "create database "
              + "testdb1"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    ODatabasePool poolNotUsed =
        youTrackDB.cachedPool(
            "testdb1",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolAdmin1 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolAdmin2 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertFalse(poolAdmin1.isClosed());
    assertEquals(poolAdmin1, poolAdmin2);

    poolAdmin1.close();

    assertTrue(poolAdmin1.isClosed());

    Thread.sleep(3_000);

    ODatabasePool poolAdmin3 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolAdmin1, poolAdmin3);
    assertFalse(poolAdmin3.isClosed());

    ODatabasePool poolOther =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolNotUsed, poolOther);
    assertTrue(poolNotUsed.isClosed());

    youTrackDB.close();
  }

  @Test
  public void testInvalidatePoolCache() {
    final YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    final YouTrackDB youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + "testdb"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");

    ODatabasePool poolAdmin1 =
        youTrackDB.cachedPool("testdb", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabasePool poolAdmin2 =
        youTrackDB.cachedPool("testdb", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertEquals(poolAdmin1, poolAdmin2);

    youTrackDB.invalidateCachedPools();

    poolAdmin1 = youTrackDB.cachedPool("testdb", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotEquals(poolAdmin2, poolAdmin1);
    youTrackDB.close();
  }

  @Test
  public void testOpenKeepClean() {
    YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()) + "keepClean",
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try {
      youTrackDb.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    } catch (Exception e) {
      // ignore
    }
    assertFalse(youTrackDb.exists("test"));

    youTrackDb.close();
  }

  @Test
  public void testYouTrackDBDatabaseOnlyMemory() {
    final YouTrackDB youTrackDb =
        OCreateDatabaseUtil.createDatabase("test",
            DBTestBase.embeddedDBUrl(getClass()) + "testYouTrackDBDatabaseOnlyMemory",
            OCreateDatabaseUtil.TYPE_MEMORY);
    final var db =
        (ODatabaseSessionInternal)
            youTrackDb.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    db.executeInTx(() -> db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId())));
    db.close();
    youTrackDb.close();
  }

  @Test
  public void createForceCloseOpen() {
    try (final YouTrackDB youTrackDB =
        OCreateDatabaseUtil.createDatabase(
            "testCreateForceCloseOpen", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_PLOCAL)) {
      youTrackDB.getInternal().forceDatabaseClose("test");
      ODatabaseSession db1 =
          youTrackDB.open(
              "testCreateForceCloseOpen", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      assertFalse(db1.isClosed());
      db1.close();
      youTrackDB.drop("testCreateForceCloseOpen");
    }
  }

  @Test
  @Ignore
  public void autoClose() throws InterruptedException {
    YouTrackDB youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    YouTrackDBEmbedded embedded = ((YouTrackDBEmbedded) YouTrackDBInternal.extract(youTrackDB));
    embedded.initAutoClose(3000);
    youTrackDB.execute(
        "create database "
            + "test"
            + " "
            + "plocal"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    ODatabaseSession db1 = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertFalse(db1.isClosed());
    db1.close();
    assertNotNull(embedded.getStorage("test"));
    Thread.sleep(4100);
    assertNull(embedded.getStorage("test"));
    youTrackDB.drop("test");
    youTrackDB.close();
  }

  @Test(expected = OStorageDoesNotExistException.class)
  public void testOpenNotExistDatabase() {
    try (YouTrackDB youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build())) {
      youTrackDB.open("testOpenNotExistDatabase", "two", "three");
    }
  }

  @Test
  public void testExecutor() throws ExecutionException, InterruptedException {
    try (YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDb.create("testExecutor", ODatabaseType.MEMORY);
      YouTrackDBInternal internal = YouTrackDBInternal.extract(youTrackDb);
      Future<Boolean> result =
          internal.execute(
              "testExecutor",
              "admin",
              (session) -> !session.isClosed() || session.getUser() != null);

      assertTrue(result.get());
    }
  }

  @Test
  public void testExecutorNoAuthorization() throws ExecutionException, InterruptedException {

    try (YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDb.create("testExecutorNoAuthorization", ODatabaseType.MEMORY);
      YouTrackDBInternal internal = YouTrackDBInternal.extract(youTrackDb);
      Future<Boolean> result =
          internal.executeNoAuthorizationAsync(
              "testExecutorNoAuthorization",
              (session) -> !session.isClosed() || session.getUser() == null);

      assertTrue(result.get());
    }
  }

  @Test
  public void testScheduler() throws InterruptedException {
    YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    YouTrackDBInternal internal = YouTrackDBInternal.extract(youTrackDb);
    CountDownLatch latch = new CountDownLatch(2);
    internal.schedule(
        new TimerTask() {
          @Override
          public void run() {
            latch.countDown();
          }
        },
        10,
        10);

    assertTrue(latch.await(5, TimeUnit.MINUTES));

    CountDownLatch once = new CountDownLatch(1);
    internal.scheduleOnce(
        new TimerTask() {
          @Override
          public void run() {
            once.countDown();
          }
        },
        10);

    assertTrue(once.await(5, TimeUnit.MINUTES));
  }

  @Test
  public void testUUID() {
    try (final YouTrackDB youTrackDb =
        OCreateDatabaseUtil.createDatabase(
            "testUUID", DBTestBase.embeddedDBUrl(getClass()), OCreateDatabaseUtil.TYPE_MEMORY)) {
      final ODatabaseSession session =
          youTrackDb.open("testUUID", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      assertNotNull(
          ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) session).getStorage())
              .getUuid());
      session.close();
    }
  }

  @Test
  public void testPersistentUUID() {
    final YouTrackDB youTrackDb =
        OCreateDatabaseUtil.createDatabase(
            "testPersistentUUID", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_PLOCAL);
    final ODatabaseSession session =
        youTrackDb.open("testPersistentUUID", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    UUID uuid =
        ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) session).getStorage()).getUuid();
    assertNotNull(uuid);
    session.close();
    youTrackDb.close();

    YouTrackDB youTrackDb1 =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabaseSession session1 =
        youTrackDb1.open("testPersistentUUID", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertEquals(
        uuid,
        ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) session1).getStorage()).getUuid());
    session1.close();
    youTrackDb1.drop("testPersistentUUID");
    youTrackDb1.close();
  }

  @Test
  public void testCreateDatabaseViaSQL() {
    String dbName = "testCreateDatabaseViaSQL";
    YouTrackDB youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    try (OResultSet result = youTrackDb.execute("create database " + dbName + " plocal")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(youTrackDb.exists(dbName));

    youTrackDb.drop(dbName);
    youTrackDb.close();
  }

  @Test
  public void testCreateDatabaseViaSQLWithUsers() {
    YouTrackDB youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()) + "testCreateDatabaseViaSQLWithUsers",
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    youTrackDB.execute(
        "create database test memory users(admin identified by 'adminpwd' role admin)");
    try (ODatabaseSession session = youTrackDB.open("test", "admin", "adminpwd")) {
    }

    youTrackDB.close();
  }

  @Test
  public void testCreateDatabaseViaSQLIfNotExistsWithUsers() {
    final YouTrackDB youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()) + "testCreateDatabaseViaSQLIfNotExistsWithUsers",
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    youTrackDB.execute(
        "create database test memory if not exists users(admin identified by 'adminpwd' role"
            + " admin)");

    youTrackDB.execute(
        "create database test memory if not exists users(admin identified by 'adminpwd' role"
            + " admin)");

    try (ODatabaseSession session = youTrackDB.open("test", "admin", "adminpwd")) {
    }

    youTrackDB.close();
  }
}
