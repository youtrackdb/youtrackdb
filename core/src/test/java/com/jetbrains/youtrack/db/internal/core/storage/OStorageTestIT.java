package com.jetbrains.youtrack.db.internal.core.storage;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OConstants;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.OSharedContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.exception.YTStorageException;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadata;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.jetbrains.youtrack.db.internal.core.storage.cache.OWriteCache;
import com.jetbrains.youtrack.db.internal.core.storage.disk.OLocalPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.fs.OFile;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.ODurablePage;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OStorageTestIT {

  private YouTrackDB youTrackDB;

  private static Path buildPath;

  @BeforeClass
  public static void beforeClass() throws IOException {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildPath = Paths.get(buildDirectory).resolve("databases")
        .resolve(OStorageTestIT.class.getSimpleName());
    Files.createDirectories(buildPath);
  }

  @Test
  public void testCheckSumFailureReadOnly() throws Exception {

    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(
                GlobalConfiguration.STORAGE_CHECKSUM_MODE,
                OChecksumMode.StoreAndSwitchReadOnlyMode)
            .addAttribute(ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var session =
        (YTDatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    OMetadata metadata = session.getMetadata();
    YTSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage =
        (OLocalPaginatedStorage) session.getStorage();
    OWriteCache wowCache = storage.getWriteCache();
    OSharedContext ctx = session.getSharedContext();
    session.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.shutdown();
    ctx.close();

    int position = 3 * 1024;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);

    int bt = file.read();
    file.seek(position);
    file.write(bt + 1);
    file.close();

    session = (YTDatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(),
        "admin", "admin");
    try {
      session.query("select from PageBreak").close();
      Assert.fail();
    } catch (YTStorageException e) {
      youTrackDB.close();
      youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
      youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    }
  }

  @Test
  public void testCheckMagicNumberReadOnly() throws Exception {
    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(
                GlobalConfiguration.STORAGE_CHECKSUM_MODE,
                OChecksumMode.StoreAndSwitchReadOnlyMode)
            .addAttribute(ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var session =
        (YTDatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    OMetadata metadata = session.getMetadata();
    YTSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage =
        (OLocalPaginatedStorage) session.getStorage();
    OWriteCache wowCache = storage.getWriteCache();
    OSharedContext ctx = session.getSharedContext();
    session.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.shutdown();
    ctx.close();

    int position = OFile.HEADER_SIZE + ODurablePage.MAGIC_NUMBER_OFFSET;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);
    file.write(1);
    file.close();

    session = (YTDatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(),
        "admin", "admin");
    try {
      session.query("select from PageBreak").close();
      Assert.fail();
    } catch (YTStorageException e) {
      youTrackDB.close();
      youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
      youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    }
  }

  @Test
  public void testCheckMagicNumberVerify() throws Exception {

    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.StoreAndVerify)
            .addAttribute(ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var session =
        (YTDatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    OMetadata metadata = session.getMetadata();
    YTSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage =
        (OLocalPaginatedStorage) session.getStorage();
    OWriteCache wowCache = storage.getWriteCache();
    OSharedContext ctx = session.getSharedContext();
    session.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.shutdown();
    ctx.close();

    int position = OFile.HEADER_SIZE + ODurablePage.MAGIC_NUMBER_OFFSET;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);
    file.write(1);
    file.close();

    session = (YTDatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(),
        "admin", "admin");
    session.query("select from PageBreak").close();

    Thread.sleep(100); // lets wait till event will be propagated

    EntityImpl document = new EntityImpl("PageBreak");
    document.field("value", "value");

    document.save();

    session.close();
  }

  @Test
  public void testCheckSumFailureVerifyAndLog() throws Exception {

    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.StoreAndVerify)
            .addAttribute(ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var session =
        (YTDatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    OMetadata metadata = session.getMetadata();
    YTSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("PageBreak");
      document.field("value", "value");
      document.save();
    }

    OLocalPaginatedStorage storage =
        (OLocalPaginatedStorage) session.getStorage();
    OWriteCache wowCache = storage.getWriteCache();
    OSharedContext ctx = session.getSharedContext();
    session.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.shutdown();
    ctx.close();

    int position = 3 * 1024;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);

    int bt = file.read();
    file.seek(position);
    file.write(bt + 1);
    file.close();

    session = (YTDatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(),
        "admin", "admin");
    session.query("select from PageBreak").close();

    Thread.sleep(100); // lets wait till event will be propagated

    EntityImpl document = new EntityImpl("PageBreak");
    document.field("value", "value");

    document.save();

    session.close();
  }

  @Test
  public void testCreatedVersionIsStored() {
    youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()), YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    final YTDatabaseSession session =
        youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    try (YTResultSet resultSet = session.query("SELECT FROM metadata:storage")) {
      Assert.assertTrue(resultSet.hasNext());

      final YTResult result = resultSet.next();
      Assert.assertEquals(OConstants.getVersion(), result.getProperty("createdAtVersion"));
    }
  }

  @After
  public void after() {
    youTrackDB.drop(OStorageTestIT.class.getSimpleName());
  }
}
