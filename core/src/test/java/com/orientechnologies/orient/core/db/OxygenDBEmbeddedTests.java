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
public class OxygenDBEmbeddedTests {

  @Test
  public void testCompatibleUrl() {
    try (OxygenDB oxygenDb = new OxygenDB(
        "plocal:" + DBTestBase.getDirectoryPath(getClass()) + "compatibleUrl",
        OxygenDBConfig.defaultConfig())) {
    }
    try (OxygenDB oxygenDb = new OxygenDB(
        "memory:" + DBTestBase.getDirectoryPath(getClass()) + "compatibleUrl",
        OxygenDBConfig.defaultConfig())) {
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongUrlFalure() {
    try (OxygenDB wrong = new OxygenDB("wrong", OxygenDBConfig.defaultConfig())) {
    }
  }

  @Test
  public void createAndUseEmbeddedDatabase() {
    try (final OxygenDB oxygenDb =
        OCreateDatabaseUtil.createDatabase(
            "createAndUseEmbeddedDatabase", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY)) {
      final var db =
          (ODatabaseSessionInternal)
              oxygenDb.open(
                  "createAndUseEmbeddedDatabase", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      db.executeInTx(
          () -> db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId())));
      db.close();
    }
  }

  @Test(expected = ODatabaseException.class)
  public void testEmbeddedDoubleCreate() {
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    try {
      oxygenDb.create("test", ODatabaseType.MEMORY);
      oxygenDb.create("test", ODatabaseType.MEMORY);
    } finally {
      oxygenDb.close();
    }
  }

  @Test
  public void createDropEmbeddedDatabase() {
    OxygenDB oxygenDb = new OxygenDB(
        DBTestBase.embeddedDBUrl(getClass()) + "createDropEmbeddedDatabase",
        OxygenDBConfig.defaultConfig());
    try {
      oxygenDb.create("test", ODatabaseType.MEMORY);
      assertTrue(oxygenDb.exists("test"));
      oxygenDb.drop("test");
      assertFalse(oxygenDb.exists("test"));
    } finally {
      oxygenDb.close();
    }
  }

  @Test
  public void testMultiThread() {
    try (final OxygenDB oxygenDb =
        OCreateDatabaseUtil.createDatabase(
            "testMultiThread", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY)) {
      final ODatabasePool pool =
          new ODatabasePool(
              oxygenDb, "testMultiThread", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

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
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()) + "listTest1",
        OxygenDBConfig.defaultConfig());
    assertEquals(0, oxygenDb.list().size());
    oxygenDb.create("test", ODatabaseType.MEMORY);
    List<String> databases = oxygenDb.list();
    assertEquals(1, databases.size());
    assertTrue(databases.contains("test"));
    oxygenDb.close();
  }

  @Test
  public void testListDatabasesPersistent() {
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()) + "listTest2",
        OxygenDBConfig.defaultConfig());
    assertEquals(0, oxygenDb.list().size());
    oxygenDb.create("testListDatabase", ODatabaseType.PLOCAL);
    List<String> databases = oxygenDb.list();
    assertEquals(1, databases.size());
    assertTrue(databases.contains("testListDatabase"));
    oxygenDb.drop("testListDatabase");
    oxygenDb.close();
  }

  @Test
  public void testRegisterDatabase() {
    final OxygenDB youtrack = new OxygenDB(
        DBTestBase.embeddedDBUrl(getClass()) + "testRegisterDatabase",
        OxygenDBConfig.defaultConfig());
    try {
      youtrack.execute("create system user admin identified by 'admin' role root");

      final OxygenDBEmbedded youtrackEmbedded = (OxygenDBEmbedded) youtrack.getInternal();
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
    try (final OxygenDB oxygenDb =
        OCreateDatabaseUtil.createDatabase(
            "testCopyOpenedDatabase", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY)) {
      ODatabaseSession db1;
      try (ODatabaseSessionInternal db =
          (ODatabaseSessionInternal)
              oxygenDb.open(
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
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    oxygenDb.close();
    oxygenDb.create("test", ODatabaseType.MEMORY);
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseOpen() {
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    oxygenDb.close();
    oxygenDb.open("testUseAfterCloseOpen", "", "");
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseList() {
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    oxygenDb.close();
    oxygenDb.list();
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseExists() {
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    oxygenDb.close();
    oxygenDb.exists("");
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseOpenPoolInternal() {
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    oxygenDb.close();
    oxygenDb.openPool("", "", "", OxygenDBConfig.defaultConfig());
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseDrop() {
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    oxygenDb.close();
    oxygenDb.drop("");
  }

  @Test
  public void testPoolByUrl() {
    final OxygenDB oxygenDb =
        OCreateDatabaseUtil.createDatabase(
            "some", DBTestBase.embeddedDBUrl(getClass()) + "poolTest",
            OCreateDatabaseUtil.TYPE_PLOCAL);
    oxygenDb.close();

    final ODatabasePool pool =
        new ODatabasePool(
            DBTestBase.embeddedDBUrl(getClass()) + "poolTest/some", "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    pool.close();
  }

  @Test
  public void testDropTL() {
    final OxygenDB oxygenDb =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()),
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    if (!oxygenDb.exists("some")) {
      oxygenDb.execute(
          "create database "
              + "some"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    if (!oxygenDb.exists("some1")) {
      oxygenDb.execute(
          "create database "
              + "some1"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    var db = oxygenDb.open("some", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    oxygenDb.drop("some1");
    db.close();
    oxygenDb.close();
  }

  @Test
  public void testClosePool() {
    try (var oxygendb = OxygenDB.embedded(DBTestBase.getDirectoryPath(getClass()) + "testClosePool",
        OxygenDBConfig.defaultConfig())) {
      if (!oxygendb.exists("test")) {
        oxygendb.create("test", ODatabaseType.PLOCAL, "admin",
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
    OxygenDBConfig config =
        OxygenDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    OxygenDB oxygenDB = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()), config);
    if (!oxygenDB.exists("testdb")) {
      oxygenDB.execute(
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
        oxygenDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolAdmin2 =
        oxygenDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolReader1 =
        oxygenDB.cachedPool("testdb", "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabasePool poolReader2 =
        oxygenDB.cachedPool("testdb", "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertEquals(poolAdmin1, poolAdmin2);
    assertEquals(poolReader1, poolReader2);
    assertNotEquals(poolAdmin1, poolReader1);

    ODatabasePool poolWriter1 =
        oxygenDB.cachedPool("testdb", "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabasePool poolWriter2 =
        oxygenDB.cachedPool("testdb", "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertEquals(poolWriter1, poolWriter2);

    ODatabasePool poolAdmin3 =
        oxygenDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolAdmin1, poolAdmin3);

    oxygenDB.close();
  }

  @Test
  public void testPoolFactoryCleanUp() throws Exception {
    OxygenDBConfig config =
        OxygenDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT, 1_000)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    OxygenDB oxygenDB = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()), config);
    if (!oxygenDB.exists("testdb")) {
      oxygenDB.execute(
          "create database "
              + "testdb"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    if (!oxygenDB.exists("testdb1")) {
      oxygenDB.execute(
          "create database "
              + "testdb1"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    ODatabasePool poolNotUsed =
        oxygenDB.cachedPool(
            "testdb1",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolAdmin1 =
        oxygenDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolAdmin2 =
        oxygenDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertFalse(poolAdmin1.isClosed());
    assertEquals(poolAdmin1, poolAdmin2);

    poolAdmin1.close();

    assertTrue(poolAdmin1.isClosed());

    Thread.sleep(3_000);

    ODatabasePool poolAdmin3 =
        oxygenDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolAdmin1, poolAdmin3);
    assertFalse(poolAdmin3.isClosed());

    ODatabasePool poolOther =
        oxygenDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolNotUsed, poolOther);
    assertTrue(poolNotUsed.isClosed());

    oxygenDB.close();
  }

  @Test
  public void testInvalidatePoolCache() {
    final OxygenDBConfig config =
        OxygenDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    final OxygenDB oxygenDB = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()), config);
    oxygenDB.execute(
        "create database "
            + "testdb"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");

    ODatabasePool poolAdmin1 =
        oxygenDB.cachedPool("testdb", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabasePool poolAdmin2 =
        oxygenDB.cachedPool("testdb", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertEquals(poolAdmin1, poolAdmin2);

    oxygenDB.invalidateCachedPools();

    poolAdmin1 = oxygenDB.cachedPool("testdb", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotEquals(poolAdmin2, poolAdmin1);
    oxygenDB.close();
  }

  @Test
  public void testOpenKeepClean() {
    OxygenDB oxygenDb =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()) + "keepClean",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try {
      oxygenDb.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    } catch (Exception e) {
      // ignore
    }
    assertFalse(oxygenDb.exists("test"));

    oxygenDb.close();
  }

  @Test
  public void testYouTrackDBDatabaseOnlyMemory() {
    final OxygenDB oxygenDb =
        OCreateDatabaseUtil.createDatabase("test",
            DBTestBase.embeddedDBUrl(getClass()) + "testYouTrackDBDatabaseOnlyMemory",
            OCreateDatabaseUtil.TYPE_MEMORY);
    final var db =
        (ODatabaseSessionInternal)
            oxygenDb.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    db.executeInTx(() -> db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId())));
    db.close();
    oxygenDb.close();
  }

  @Test
  public void createForceCloseOpen() {
    try (final OxygenDB oxygenDB =
        OCreateDatabaseUtil.createDatabase(
            "testCreateForceCloseOpen", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_PLOCAL)) {
      oxygenDB.getInternal().forceDatabaseClose("test");
      ODatabaseSession db1 =
          oxygenDB.open(
              "testCreateForceCloseOpen", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      assertFalse(db1.isClosed());
      db1.close();
      oxygenDB.drop("testCreateForceCloseOpen");
    }
  }

  @Test
  @Ignore
  public void autoClose() throws InterruptedException {
    OxygenDB oxygenDB =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()),
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    OxygenDBEmbedded embedded = ((OxygenDBEmbedded) OxygenDBInternal.extract(oxygenDB));
    embedded.initAutoClose(3000);
    oxygenDB.execute(
        "create database "
            + "test"
            + " "
            + "plocal"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    ODatabaseSession db1 = oxygenDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertFalse(db1.isClosed());
    db1.close();
    assertNotNull(embedded.getStorage("test"));
    Thread.sleep(4100);
    assertNull(embedded.getStorage("test"));
    oxygenDB.drop("test");
    oxygenDB.close();
  }

  @Test(expected = OStorageDoesNotExistException.class)
  public void testOpenNotExistDatabase() {
    try (OxygenDB oxygenDB =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()),
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build())) {
      oxygenDB.open("testOpenNotExistDatabase", "two", "three");
    }
  }

  @Test
  public void testExecutor() throws ExecutionException, InterruptedException {
    try (OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig())) {
      oxygenDb.create("testExecutor", ODatabaseType.MEMORY);
      OxygenDBInternal internal = OxygenDBInternal.extract(oxygenDb);
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

    try (OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig())) {
      oxygenDb.create("testExecutorNoAuthorization", ODatabaseType.MEMORY);
      OxygenDBInternal internal = OxygenDBInternal.extract(oxygenDb);
      Future<Boolean> result =
          internal.executeNoAuthorizationAsync(
              "testExecutorNoAuthorization",
              (session) -> !session.isClosed() || session.getUser() == null);

      assertTrue(result.get());
    }
  }

  @Test
  public void testScheduler() throws InterruptedException {
    OxygenDB oxygenDb =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()),
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    OxygenDBInternal internal = OxygenDBInternal.extract(oxygenDb);
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
    try (final OxygenDB oxygenDb =
        OCreateDatabaseUtil.createDatabase(
            "testUUID", DBTestBase.embeddedDBUrl(getClass()), OCreateDatabaseUtil.TYPE_MEMORY)) {
      final ODatabaseSession session =
          oxygenDb.open("testUUID", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      assertNotNull(
          ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) session).getStorage())
              .getUuid());
      session.close();
    }
  }

  @Test
  public void testPersistentUUID() {
    final OxygenDB oxygenDb =
        OCreateDatabaseUtil.createDatabase(
            "testPersistentUUID", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_PLOCAL);
    final ODatabaseSession session =
        oxygenDb.open("testPersistentUUID", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    UUID uuid =
        ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) session).getStorage()).getUuid();
    assertNotNull(uuid);
    session.close();
    oxygenDb.close();

    OxygenDB oxygenDb1 =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()),
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabaseSession session1 =
        oxygenDb1.open("testPersistentUUID", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertEquals(
        uuid,
        ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) session1).getStorage()).getUuid());
    session1.close();
    oxygenDb1.drop("testPersistentUUID");
    oxygenDb1.close();
  }

  @Test
  public void testCreateDatabaseViaSQL() {
    String dbName = "testCreateDatabaseViaSQL";
    OxygenDB oxygenDb = new OxygenDB(DBTestBase.embeddedDBUrl(getClass()),
        OxygenDBConfig.defaultConfig());
    try (OResultSet result = oxygenDb.execute("create database " + dbName + " plocal")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(oxygenDb.exists(dbName));

    oxygenDb.drop(dbName);
    oxygenDb.close();
  }

  @Test
  public void testCreateDatabaseViaSQLWithUsers() {
    OxygenDB oxygenDB =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()) + "testCreateDatabaseViaSQLWithUsers",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    oxygenDB.execute(
        "create database test memory users(admin identified by 'adminpwd' role admin)");
    try (ODatabaseSession session = oxygenDB.open("test", "admin", "adminpwd")) {
    }

    oxygenDB.close();
  }

  @Test
  public void testCreateDatabaseViaSQLIfNotExistsWithUsers() {
    final OxygenDB oxygenDB =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()) + "testCreateDatabaseViaSQLIfNotExistsWithUsers",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    oxygenDB.execute(
        "create database test memory if not exists users(admin identified by 'adminpwd' role"
            + " admin)");

    oxygenDB.execute(
        "create database test memory if not exists users(admin identified by 'adminpwd' role"
            + " admin)");

    try (ODatabaseSession session = oxygenDB.open("test", "admin", "adminpwd")) {
    }

    oxygenDB.close();
  }
}
