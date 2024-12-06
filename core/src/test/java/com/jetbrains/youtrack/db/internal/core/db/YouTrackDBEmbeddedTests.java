package com.jetbrains.youtrack.db.internal.core.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.StorageDoesNotExistException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
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
        "plocal:" + DbTestBase.getDirectoryPath(getClass()) + "compatibleUrl",
        YouTrackDBConfig.defaultConfig())) {
    }
    try (YouTrackDB youTrackDb = new YouTrackDB(
        "memory:" + DbTestBase.getDirectoryPath(getClass()) + "compatibleUrl",
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
        CreateDatabaseUtil.createDatabase(
            "createAndUseEmbeddedDatabase", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY)) {
      final var db =
          (DatabaseSessionInternal)
              youTrackDb.open(
                  "createAndUseEmbeddedDatabase", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      db.executeInTx(
          () -> db.save(new EntityImpl(), db.getClusterNameById(db.getDefaultClusterId())));
      db.close();
    }
  }

  @Test(expected = DatabaseException.class)
  public void testEmbeddedDoubleCreate() {
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    try {
      youTrackDb.create("test", DatabaseType.MEMORY);
      youTrackDb.create("test", DatabaseType.MEMORY);
    } finally {
      youTrackDb.close();
    }
  }

  @Test
  public void createDropEmbeddedDatabase() {
    YouTrackDB youTrackDb = new YouTrackDB(
        DbTestBase.embeddedDBUrl(getClass()) + "createDropEmbeddedDatabase",
        YouTrackDBConfig.defaultConfig());
    try {
      youTrackDb.create("test", DatabaseType.MEMORY);
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
        CreateDatabaseUtil.createDatabase(
            "testMultiThread", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY)) {
      final DatabasePool pool =
          new DatabasePool(
              youTrackDb, "testMultiThread", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      // do a query and assert on other thread
      Runnable acquirer =
          () -> {
            DatabaseSession db = pool.acquire();
            try {
              assertThat(db.isActiveOnCurrentThread()).isTrue();
              final ResultSet res = db.query("SELECT * FROM OUser");
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
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()) + "listTest1",
        YouTrackDBConfig.defaultConfig());
    assertEquals(0, youTrackDb.list().size());
    youTrackDb.create("test", DatabaseType.MEMORY);
    List<String> databases = youTrackDb.list();
    assertEquals(1, databases.size());
    assertTrue(databases.contains("test"));
    youTrackDb.close();
  }

  @Test
  public void testListDatabasesPersistent() {
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()) + "listTest2",
        YouTrackDBConfig.defaultConfig());
    assertEquals(0, youTrackDb.list().size());
    youTrackDb.create("testListDatabase", DatabaseType.PLOCAL);
    List<String> databases = youTrackDb.list();
    assertEquals(1, databases.size());
    assertTrue(databases.contains("testListDatabase"));
    youTrackDb.drop("testListDatabase");
    youTrackDb.close();
  }

  @Test
  public void testRegisterDatabase() {
    final YouTrackDB youtrack = new YouTrackDB(
        DbTestBase.embeddedDBUrl(getClass()) + "testRegisterDatabase",
        YouTrackDBConfig.defaultConfig());
    try {
      youtrack.execute("create system user admin identified by 'admin' role root");

      final YouTrackDBEmbedded youtrackEmbedded = (YouTrackDBEmbedded) youtrack.getInternal();
      assertEquals(0, youtrackEmbedded.listDatabases("", "").size());
      youtrackEmbedded.initCustomStorage("database1", DbTestBase.getDirectoryPath(getClass()) +
              "testRegisterDatabase/database1",
          "", "");
      try (final DatabaseSession db = youtrackEmbedded.open("database1", "admin", "admin")) {
        assertEquals("database1", db.getName());
      }
      youtrackEmbedded.initCustomStorage("database2", DbTestBase.getDirectoryPath(getClass()) +
              "testRegisterDatabase/database2",
          "", "");

      try (final DatabaseSession db = youtrackEmbedded.open("database2", "admin", "admin")) {
        assertEquals("database2", db.getName());
      }
      youtrackEmbedded.drop("database1", null, null);
      youtrackEmbedded.drop("database2", null, null);
      youtrackEmbedded.close();
    } finally {
      FileUtils.deleteRecursively(
          new File(DbTestBase.getDirectoryPath(getClass()) + "testRegisterDatabase"));
    }
  }

  @Test
  public void testCopyOpenedDatabase() {
    try (final YouTrackDB youTrackDb =
        CreateDatabaseUtil.createDatabase(
            "testCopyOpenedDatabase", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY)) {
      DatabaseSession db1;
      try (DatabaseSessionInternal db =
          (DatabaseSessionInternal)
              youTrackDb.open(
                  "testCopyOpenedDatabase", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        db1 = db.copy();
      }
      db1.activateOnCurrentThread();
      assertFalse(db1.isClosed());
      db1.close();
    }
  }

  @Test(expected = DatabaseException.class)
  public void testUseAfterCloseCreate() {
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.create("test", DatabaseType.MEMORY);
  }

  @Test(expected = DatabaseException.class)
  public void testUseAfterCloseOpen() {
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.open("testUseAfterCloseOpen", "", "");
  }

  @Test(expected = DatabaseException.class)
  public void testUseAfterCloseList() {
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.list();
  }

  @Test(expected = DatabaseException.class)
  public void testUseAfterCloseExists() {
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.exists("");
  }

  @Test(expected = DatabaseException.class)
  public void testUseAfterCloseOpenPoolInternal() {
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.openPool("", "", "", YouTrackDBConfig.defaultConfig());
  }

  @Test(expected = DatabaseException.class)
  public void testUseAfterCloseDrop() {
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDb.close();
    youTrackDb.drop("");
  }

  @Test
  public void testPoolByUrl() {
    final YouTrackDB youTrackDb =
        CreateDatabaseUtil.createDatabase(
            "some", DbTestBase.embeddedDBUrl(getClass()) + "poolTest",
            CreateDatabaseUtil.TYPE_PLOCAL);
    youTrackDb.close();

    final DatabasePool pool =
        new DatabasePool(
            DbTestBase.embeddedDBUrl(getClass()) + "poolTest/some", "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    pool.close();
  }

  @Test
  public void testDropTL() {
    final YouTrackDB youTrackDb =
        new YouTrackDB(
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    if (!youTrackDb.exists("some")) {
      youTrackDb.execute(
          "create database "
              + "some"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    if (!youTrackDb.exists("some1")) {
      youTrackDb.execute(
          "create database "
              + "some1"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    var db = youTrackDb.open("some", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    youTrackDb.drop("some1");
    db.close();
    youTrackDb.close();
  }

  @Test
  public void testClosePool() {
    try (var youTrackDB = YouTrackDB.embedded(
        DbTestBase.getDirectoryPath(getClass()) + "testClosePool",
        YouTrackDBConfig.defaultConfig())) {
      if (!youTrackDB.exists("test")) {
        youTrackDB.create("test", DatabaseType.PLOCAL, "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD, "admin");
      }
    }

    final DatabasePool pool =
        new DatabasePool(
            DbTestBase.embeddedDBUrl(getClass()) + "testClosePool/test",
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertFalse(pool.isClosed());
    pool.close();
    assertTrue(pool.isClosed());

  }

  @Test
  public void testPoolFactory() {
    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    YouTrackDB youTrackDB = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()), config);
    if (!youTrackDB.exists("testdb")) {
      youTrackDB.execute(
          "create database "
              + "testdb"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin, reader identified by '"
              + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role reader, writer identified by '"
              + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role writer)");
    }
    DatabasePool poolAdmin1 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    DatabasePool poolAdmin2 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    DatabasePool poolReader1 =
        youTrackDB.cachedPool("testdb", "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    DatabasePool poolReader2 =
        youTrackDB.cachedPool("testdb", "reader", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertEquals(poolAdmin1, poolAdmin2);
    assertEquals(poolReader1, poolReader2);
    assertNotEquals(poolAdmin1, poolReader1);

    DatabasePool poolWriter1 =
        youTrackDB.cachedPool("testdb", "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    DatabasePool poolWriter2 =
        youTrackDB.cachedPool("testdb", "writer", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertEquals(poolWriter1, poolWriter2);

    DatabasePool poolAdmin3 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolAdmin1, poolAdmin3);

    youTrackDB.close();
  }

  @Test
  public void testPoolFactoryCleanUp() throws Exception {
    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(GlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT, 1_000)
            .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    YouTrackDB youTrackDB = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()), config);
    if (!youTrackDB.exists("testdb")) {
      youTrackDB.execute(
          "create database "
              + "testdb"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    if (!youTrackDB.exists("testdb1")) {
      youTrackDB.execute(
          "create database "
              + "testdb1"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    DatabasePool poolNotUsed =
        youTrackDB.cachedPool(
            "testdb1",
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    DatabasePool poolAdmin1 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    DatabasePool poolAdmin2 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertFalse(poolAdmin1.isClosed());
    assertEquals(poolAdmin1, poolAdmin2);

    poolAdmin1.close();

    assertTrue(poolAdmin1.isClosed());

    Thread.sleep(3_000);

    DatabasePool poolAdmin3 =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolAdmin1, poolAdmin3);
    assertFalse(poolAdmin3.isClosed());

    DatabasePool poolOther =
        youTrackDB.cachedPool(
            "testdb",
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolNotUsed, poolOther);
    assertTrue(poolNotUsed.isClosed());

    youTrackDB.close();
  }

  @Test
  public void testInvalidatePoolCache() {
    final YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    final YouTrackDB youTrackDB = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + "testdb"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");

    DatabasePool poolAdmin1 =
        youTrackDB.cachedPool("testdb", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    DatabasePool poolAdmin2 =
        youTrackDB.cachedPool("testdb", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertEquals(poolAdmin1, poolAdmin2);

    youTrackDB.invalidateCachedPools();

    poolAdmin1 = youTrackDB.cachedPool("testdb", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotEquals(poolAdmin2, poolAdmin1);
    youTrackDB.close();
  }

  @Test
  public void testOpenKeepClean() {
    YouTrackDB youTrackDb =
        new YouTrackDB(
            DbTestBase.embeddedDBUrl(getClass()) + "keepClean",
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try {
      youTrackDb.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    } catch (Exception e) {
      // ignore
    }
    assertFalse(youTrackDb.exists("test"));

    youTrackDb.close();
  }

  @Test
  public void testYouTrackDBDatabaseOnlyMemory() {
    final YouTrackDB youTrackDb =
        CreateDatabaseUtil.createDatabase("test",
            DbTestBase.embeddedDBUrl(getClass()) + "testYouTrackDBDatabaseOnlyMemory",
            CreateDatabaseUtil.TYPE_MEMORY);
    final var db =
        (DatabaseSessionInternal)
            youTrackDb.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    db.executeInTx(
        () -> db.save(new EntityImpl(), db.getClusterNameById(db.getDefaultClusterId())));
    db.close();
    youTrackDb.close();
  }

  @Test
  public void createForceCloseOpen() {
    try (final YouTrackDB youTrackDB =
        CreateDatabaseUtil.createDatabase(
            "testCreateForceCloseOpen", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_PLOCAL)) {
      youTrackDB.getInternal().forceDatabaseClose("test");
      DatabaseSession db1 =
          youTrackDB.open(
              "testCreateForceCloseOpen", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    YouTrackDBEmbedded embedded = ((YouTrackDBEmbedded) YouTrackDBInternal.extract(youTrackDB));
    embedded.initAutoClose(3000);
    youTrackDB.execute(
        "create database "
            + "test"
            + " "
            + "plocal"
            + " users ( admin identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    DatabaseSession db1 = youTrackDB.open("test", "admin",
        CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertFalse(db1.isClosed());
    db1.close();
    assertNotNull(embedded.getStorage("test"));
    Thread.sleep(4100);
    assertNull(embedded.getStorage("test"));
    youTrackDB.drop("test");
    youTrackDB.close();
  }

  @Test(expected = StorageDoesNotExistException.class)
  public void testOpenNotExistDatabase() {
    try (YouTrackDB youTrackDB =
        new YouTrackDB(
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build())) {
      youTrackDB.open("testOpenNotExistDatabase", "two", "three");
    }
  }

  @Test
  public void testExecutor() throws ExecutionException, InterruptedException {
    try (YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDb.create("testExecutor", DatabaseType.MEMORY);
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

    try (YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDb.create("testExecutorNoAuthorization", DatabaseType.MEMORY);
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
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
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
        CreateDatabaseUtil.createDatabase(
            "testUUID", DbTestBase.embeddedDBUrl(getClass()), CreateDatabaseUtil.TYPE_MEMORY)) {
      final DatabaseSession session =
          youTrackDb.open("testUUID", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      assertNotNull(
          ((AbstractPaginatedStorage) ((DatabaseSessionInternal) session).getStorage())
              .getUuid());
      session.close();
    }
  }

  @Test
  public void testPersistentUUID() {
    final YouTrackDB youTrackDb =
        CreateDatabaseUtil.createDatabase(
            "testPersistentUUID", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_PLOCAL);
    final DatabaseSession session =
        youTrackDb.open("testPersistentUUID", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    UUID uuid =
        ((AbstractPaginatedStorage) ((DatabaseSessionInternal) session).getStorage()).getUuid();
    assertNotNull(uuid);
    session.close();
    youTrackDb.close();

    YouTrackDB youTrackDb1 =
        new YouTrackDB(
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    DatabaseSession session1 =
        youTrackDb1.open("testPersistentUUID", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertEquals(
        uuid,
        ((AbstractPaginatedStorage) ((DatabaseSessionInternal) session1).getStorage()).getUuid());
    session1.close();
    youTrackDb1.drop("testPersistentUUID");
    youTrackDb1.close();
  }

  @Test
  public void testCreateDatabaseViaSQL() {
    String dbName = "testCreateDatabaseViaSQL";
    YouTrackDB youTrackDb = new YouTrackDB(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    try (ResultSet result = youTrackDb.execute("create database " + dbName + " plocal")) {
      Assert.assertTrue(result.hasNext());
      Result item = result.next();
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
            DbTestBase.embeddedDBUrl(getClass()) + "testCreateDatabaseViaSQLWithUsers",
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    youTrackDB.execute(
        "create database test memory users(admin identified by 'adminpwd' role admin)");
    try (DatabaseSession session = youTrackDB.open("test", "admin", "adminpwd")) {
    }

    youTrackDB.close();
  }

  @Test
  public void testCreateDatabaseViaSQLIfNotExistsWithUsers() {
    final YouTrackDB youTrackDB =
        new YouTrackDB(
            DbTestBase.embeddedDBUrl(getClass()) + "testCreateDatabaseViaSQLIfNotExistsWithUsers",
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    youTrackDB.execute(
        "create database test memory if not exists users(admin identified by 'adminpwd' role"
            + " admin)");

    youTrackDB.execute(
        "create database test memory if not exists users(admin identified by 'adminpwd' role"
            + " admin)");

    try (DatabaseSession session = youTrackDB.open("test", "admin", "adminpwd")) {
    }

    youTrackDB.close();
  }
}
