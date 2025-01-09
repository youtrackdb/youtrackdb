package com.jetbrains.youtrack.db.internal.core.storage;

import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigImpl;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.File;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class StorageEncryptionTestIT {

  @Test
  public void testEncryption() {
    final File dbDirectoryFile = cleanAndGetDirectory();

    final YouTrackDBConfig youTrackDBConfig =
        YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "T1JJRU5UREJfSVNfQ09PTA==")
            .build();
    try (final YouTrackDB youTrackDB =
        new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), youTrackDBConfig)) {
      youTrackDB.execute(
          "create database encryption plocal users ( admin identified by 'admin' role admin)");
      try (var session = (DatabaseSessionInternal) youTrackDB.open("encryption", "admin",
          "admin")) {
        final Schema schema = session.getMetadata().getSchema();
        final SchemaClass cls = schema.createClass("EncryptedData");
        cls.createProperty(session, "id", PropertyType.INTEGER);
        cls.createProperty(session, "value", PropertyType.STRING);

        cls.createIndex(session, "EncryptedTree", SchemaClass.INDEX_TYPE.UNIQUE, "id");
        cls.createIndex(session, "EncryptedHash", SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "id");

        for (int i = 0; i < 10_000; i++) {
          final EntityImpl document = new EntityImpl(cls);
          document.setProperty("id", i);
          document.setProperty(
              "value",
              "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor"
                  + " incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis"
                  + " nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
                  + " ");
          document.save();
        }

        final Random random = ThreadLocalRandom.current();
        for (int i = 0; i < 1_000; i++) {
          try (ResultSet resultSet =
              session.query("select from EncryptedData where id = ?", random.nextInt(10_000_000))) {
            if (resultSet.hasNext()) {
              final Result result = resultSet.next();
              result.getEntity().ifPresent(DBRecord::delete);
            }
          }
        }
      }
    }

    try (final YouTrackDB youTrackDB =
        new YouTrackDBImpl(
            DbTestBase.embeddedDBUrl(getClass()), YouTrackDBConfig.defaultConfig())) {
      try {
        try (final DatabaseSession session = youTrackDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    final YouTrackDBConfig wrongKeyOneYouTrackDBConfig =
        YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(
                GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8=")
            .build();
    try (final YouTrackDB youTrackDB =
        new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), wrongKeyOneYouTrackDBConfig)) {
      try {
        try (final DatabaseSession session = youTrackDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    final YouTrackDBConfig wrongKeyTwoYouTrackDBConfig =
        YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(
                GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8")
            .build();
    try (final YouTrackDB youTrackDB =
        new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), wrongKeyTwoYouTrackDBConfig)) {
      try {
        try (final DatabaseSession session = youTrackDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    try (final YouTrackDB youTrackDB =
        new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()), youTrackDBConfig)) {
      try (final DatabaseSessionInternal session =
          (DatabaseSessionInternal) youTrackDB.open("encryption", "admin", "admin")) {
        final IndexManagerAbstract indexManager = session.getMetadata().getIndexManagerInternal();
        final Index treeIndex = indexManager.getIndex(session, "EncryptedTree");
        final Index hashIndex = indexManager.getIndex(session, "EncryptedHash");

        for (final EntityImpl document : session.browseClass("EncryptedData")) {
          final int id = document.getProperty("id");
          final RID treeRid;
          try (Stream<RID> rids = treeIndex.getInternal().getRids(session, id)) {
            treeRid = rids.findFirst().orElse(null);
          }
          final RID hashRid;
          try (Stream<RID> rids = hashIndex.getInternal().getRids(session, id)) {
            hashRid = rids.findFirst().orElse(null);
          }

          Assert.assertEquals(document.getIdentity(), treeRid);
          Assert.assertEquals(document.getIdentity(), hashRid);
        }

        Assert.assertEquals(session.countClass("EncryptedData"),
            treeIndex.getInternal().size(session));
        Assert.assertEquals(session.countClass("EncryptedData"),
            hashIndex.getInternal().size(session));
      }
    }
  }

  private File cleanAndGetDirectory() {
    final String dbDirectory =
        "./target/databases" + File.separator + StorageEncryptionTestIT.class.getSimpleName();
    final File dbDirectoryFile = new File(dbDirectory);
    FileUtils.deleteRecursively(dbDirectoryFile);
    return dbDirectoryFile;
  }

  @Test
  public void testEncryptionSingleDatabase() {
    final File dbDirectoryFile = cleanAndGetDirectory();

    try (final YouTrackDB youTrackDB =
        new YouTrackDBImpl(
            DbTestBase.embeddedDBUrl(getClass()), YouTrackDBConfig.defaultConfig())) {
      final YouTrackDBConfig youTrackDBConfig =
          YouTrackDBConfig.builder()
              .addGlobalConfigurationParameter(GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                  "T1JJRU5UREJfSVNfQ09PTA==")
              .build();

      youTrackDB.execute(
          "create database encryption plocal users ( admin identified by 'admin' role admin)");
    }
    try (final YouTrackDB youTrackDB =
        new YouTrackDBImpl(
            DbTestBase.embeddedDBUrl(getClass()), YouTrackDBConfig.defaultConfig())) {
      final YouTrackDBConfigImpl youTrackDBConfig =
          (YouTrackDBConfigImpl) YouTrackDBConfig.builder()
              .addGlobalConfigurationParameter(GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                  "T1JJRU5UREJfSVNfQ09PTA==")
              .build();
      try (var session =
          (DatabaseSessionInternal) youTrackDB.open("encryption", "admin", "admin",
              youTrackDBConfig)) {
        final Schema schema = session.getMetadata().getSchema();
        final SchemaClass cls = schema.createClass("EncryptedData");

        final EntityImpl document = new EntityImpl(cls);
        document.setProperty("id", 10);
        document.setProperty(
            "value",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor"
                + " incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis"
                + " nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
                + " ");
        document.save();

        try (ResultSet resultSet = session.query("select from EncryptedData where id = ?", 10)) {
          assertTrue(resultSet.hasNext());
        }
      }
    }
  }
}
