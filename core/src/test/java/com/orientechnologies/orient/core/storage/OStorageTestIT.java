package com.orientechnologies.orient.core.storage;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
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
                OGlobalConfiguration.STORAGE_CHECKSUM_MODE,
                OChecksumMode.StoreAndSwitchReadOnlyMode)
            .addAttribute(ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var session =
        (ODatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    OMetadata metadata = session.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
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

    session = (ODatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(),
        "admin", "admin");
    try {
      session.query("select from PageBreak").close();
      Assert.fail();
    } catch (OStorageException e) {
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
                OGlobalConfiguration.STORAGE_CHECKSUM_MODE,
                OChecksumMode.StoreAndSwitchReadOnlyMode)
            .addAttribute(ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var session =
        (ODatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    OMetadata metadata = session.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
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

    session = (ODatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(),
        "admin", "admin");
    try {
      session.query("select from PageBreak").close();
      Assert.fail();
    } catch (OStorageException e) {
      youTrackDB.close();
      youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
      youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    }
  }

  @Test
  public void testCheckMagicNumberVerify() throws Exception {

    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.StoreAndVerify)
            .addAttribute(ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var session =
        (ODatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    OMetadata metadata = session.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
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

    session = (ODatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(),
        "admin", "admin");
    session.query("select from PageBreak").close();

    Thread.sleep(100); // lets wait till event will be propagated

    ODocument document = new ODocument("PageBreak");
    document.field("value", "value");

    document.save();

    session.close();
  }

  @Test
  public void testCheckSumFailureVerifyAndLog() throws Exception {

    YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_CHECKSUM_MODE, OChecksumMode.StoreAndVerify)
            .addAttribute(ATTRIBUTES.MINIMUMCLUSTERS, 1)
            .build();

    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), config);
    youTrackDB.execute(
        "create database "
            + OStorageTestIT.class.getSimpleName()
            + " plocal users ( admin identified by 'admin' role admin)");

    var session =
        (ODatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin",
            "admin", config);
    OMetadata metadata = session.getMetadata();
    OSchema schema = metadata.getSchema();
    schema.createClass("PageBreak");

    for (int i = 0; i < 10; i++) {
      ODocument document = new ODocument("PageBreak");
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

    session = (ODatabaseSessionInternal) youTrackDB.open(OStorageTestIT.class.getSimpleName(),
        "admin", "admin");
    session.query("select from PageBreak").close();

    Thread.sleep(100); // lets wait till event will be propagated

    ODocument document = new ODocument("PageBreak");
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

    final ODatabaseSession session =
        youTrackDB.open(OStorageTestIT.class.getSimpleName(), "admin", "admin");
    try (OResultSet resultSet = session.query("SELECT FROM metadata:storage")) {
      Assert.assertTrue(resultSet.hasNext());

      final OResult result = resultSet.next();
      Assert.assertEquals(OConstants.getVersion(), result.getProperty("createdAtVersion"));
    }
  }

  @After
  public void after() {
    youTrackDB.drop(OStorageTestIT.class.getSimpleName());
  }
}
