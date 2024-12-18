package com.jetbrains.youtrack.db.internal.core.storage;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBConstants;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.SharedContext;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.exception.StorageException;
import com.jetbrains.youtrack.db.internal.core.metadata.Metadata;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.cache.WriteCache;
import com.jetbrains.youtrack.db.internal.core.storage.disk.LocalPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.fs.File;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurablePage;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class StorageTestIT {

  private YouTrackDB youTrackDB;

  private static Path buildPath;

  @BeforeClass
  public static void beforeClass() throws IOException {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildPath = Paths.get(buildDirectory).resolve("databases")
        .resolve(StorageTestIT.class.getSimpleName());
    Files.createDirectories(buildPath);
  }

  @Test
  public void testCheckSumFailureReadOnly() throws Exception {

    YouTrackDBConfigImpl config =
        (YouTrackDBConfigImpl) YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(
                GlobalConfiguration.STORAGE_CHECKSUM_MODE,
                ChecksumMode.StoreAndSwitchReadOnlyMode)
            .addAttribute(DatabaseSession.ATTRIBUTES.MINIMUM_CLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + StorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var session =
        (DatabaseSessionInternal) youTrackDB.open(StorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    Metadata metadata = session.getMetadata();
    Schema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = ((EntityImpl) session.newEntity("PageBreak"));
      document.field("value", "value");
      document.save();
    }

    LocalPaginatedStorage storage =
        (LocalPaginatedStorage) session.getStorage();
    WriteCache wowCache = storage.getWriteCache();
    SharedContext ctx = session.getSharedContext();
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

    session = (DatabaseSessionInternal) youTrackDB.open(StorageTestIT.class.getSimpleName(),
        "admin", "admin");
    try {
      session.query("select from PageBreak").close();
      Assert.fail();
    } catch (StorageException e) {
      youTrackDB.close();
      youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), config);
      youTrackDB.open(StorageTestIT.class.getSimpleName(), "admin", "admin");
    }
  }

  @Test
  public void testCheckMagicNumberReadOnly() throws Exception {
    YouTrackDBConfigImpl config =
        (YouTrackDBConfigImpl) YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(
                GlobalConfiguration.STORAGE_CHECKSUM_MODE,
                ChecksumMode.StoreAndSwitchReadOnlyMode)
            .addAttribute(DatabaseSession.ATTRIBUTES.MINIMUM_CLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + StorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var db =
        (DatabaseSessionInternal) youTrackDB.open(StorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    Metadata metadata = db.getMetadata();
    Schema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity("PageBreak"));
      document.field("value", "value");
      document.save();
    }

    LocalPaginatedStorage storage =
        (LocalPaginatedStorage) db.getStorage();
    WriteCache wowCache = storage.getWriteCache();
    SharedContext ctx = db.getSharedContext();
    db.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.shutdown();
    ctx.close();

    int position = File.HEADER_SIZE + DurablePage.MAGIC_NUMBER_OFFSET;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);
    file.write(1);
    file.close();

    db = (DatabaseSessionInternal) youTrackDB.open(StorageTestIT.class.getSimpleName(),
        "admin", "admin");
    try {
      db.query("select from PageBreak").close();
      Assert.fail();
    } catch (StorageException e) {
      youTrackDB.close();
      youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), config);
      youTrackDB.open(StorageTestIT.class.getSimpleName(), "admin", "admin");
    }
  }

  @Test
  public void testCheckMagicNumberVerify() throws Exception {

    YouTrackDBConfigImpl config =
        (YouTrackDBConfigImpl) YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(GlobalConfiguration.STORAGE_CHECKSUM_MODE,
                ChecksumMode.StoreAndVerify)
            .addAttribute(DatabaseSession.ATTRIBUTES.MINIMUM_CLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + StorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var db =
        (DatabaseSessionInternal) youTrackDB.open(StorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    Metadata metadata = db.getMetadata();
    Schema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity("PageBreak"));
      document.field("value", "value");
      document.save();
    }

    LocalPaginatedStorage storage =
        (LocalPaginatedStorage) db.getStorage();
    WriteCache wowCache = storage.getWriteCache();
    SharedContext ctx = db.getSharedContext();
    db.close();

    final Path storagePath = storage.getStoragePath();

    long fileId = wowCache.fileIdByName("pagebreak.pcl");
    String nativeFileName = wowCache.nativeFileNameById(fileId);

    storage.shutdown();
    ctx.close();

    int position = File.HEADER_SIZE + DurablePage.MAGIC_NUMBER_OFFSET;

    RandomAccessFile file =
        new RandomAccessFile(storagePath.resolve(nativeFileName).toFile(), "rw");
    file.seek(position);
    file.write(1);
    file.close();

    db = (DatabaseSessionInternal) youTrackDB.open(StorageTestIT.class.getSimpleName(),
        "admin", "admin");
    db.query("select from PageBreak").close();

    Thread.sleep(100); // lets wait till event will be propagated

    EntityImpl document = ((EntityImpl) db.newEntity("PageBreak"));
    document.field("value", "value");

    document.save();

    db.close();
  }

  @Test
  public void testCheckSumFailureVerifyAndLog() throws Exception {

    YouTrackDBConfigImpl config =
        (YouTrackDBConfigImpl) YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(GlobalConfiguration.STORAGE_CHECKSUM_MODE,
                ChecksumMode.StoreAndVerify)
            .addAttribute(DatabaseSession.ATTRIBUTES.MINIMUM_CLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + StorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var db =
        (DatabaseSessionInternal) youTrackDB.open(StorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    Metadata metadata = db.getMetadata();
    Schema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity("PageBreak"));
      document.field("value", "value");
      document.save();
    }

    LocalPaginatedStorage storage =
        (LocalPaginatedStorage) db.getStorage();
    WriteCache wowCache = storage.getWriteCache();
    SharedContext ctx = db.getSharedContext();
    db.close();

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

    db = (DatabaseSessionInternal) youTrackDB.open(StorageTestIT.class.getSimpleName(),
        "admin", "admin");
    db.query("select from PageBreak").close();

    Thread.sleep(100); // lets wait till event will be propagated

    EntityImpl document = ((EntityImpl) db.newEntity("PageBreak"));
    document.field("value", "value");

    document.save();

    db.close();
  }

  @Test
  public void testCreatedVersionIsStored() {
    youTrackDB =
        new YouTrackDBImpl(
            DbTestBase.embeddedDBUrl(getClass()), YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database "
            + StorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    final DatabaseSession session =
        youTrackDB.open(StorageTestIT.class.getSimpleName(), "admin", "admin");
    try (ResultSet resultSet = session.query("SELECT FROM metadata:storage")) {
      Assert.assertTrue(resultSet.hasNext());

      final Result result = resultSet.next();
      Assert.assertEquals(YouTrackDBConstants.getVersion(), result.getProperty("createdAtVersion"));
    }
  }

  @After
  public void after() {
    youTrackDB.drop(StorageTestIT.class.getSimpleName());
  }
}
