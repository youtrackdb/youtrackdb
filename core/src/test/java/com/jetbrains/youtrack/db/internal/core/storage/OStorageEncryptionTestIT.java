package com.jetbrains.youtrack.db.internal.core.storage;

import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.io.OFileUtils;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.OIndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.io.File;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class OStorageEncryptionTestIT {

  @Test
  public void testEncryption() {
    final File dbDirectoryFile = cleanAndGetDirectory();

    final YouTrackDBConfig youTrackDBConfig =
        YouTrackDBConfig.builder()
            .addConfig(GlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
            .build();
    try (final YouTrackDB youTrackDB =
        new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), youTrackDBConfig)) {
      youTrackDB.execute(
          "create database encryption plocal users ( admin identified by 'admin' role admin)");
      try (var session = (YTDatabaseSessionInternal) youTrackDB.open("encryption", "admin",
          "admin")) {
        final YTSchema schema = session.getMetadata().getSchema();
        final YTClass cls = schema.createClass("EncryptedData");
        cls.createProperty(session, "id", YTType.INTEGER);
        cls.createProperty(session, "value", YTType.STRING);

        cls.createIndex(session, "EncryptedTree", YTClass.INDEX_TYPE.UNIQUE, "id");
        cls.createIndex(session, "EncryptedHash", YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "id");

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
          try (YTResultSet resultSet =
              session.query("select from EncryptedData where id = ?", random.nextInt(10_000_000))) {
            if (resultSet.hasNext()) {
              final YTResult result = resultSet.next();
              result.getEntity().ifPresent(Record::delete);
            }
          }
        }
      }
    }

    try (final YouTrackDB youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()), YouTrackDBConfig.defaultConfig())) {
      try {
        try (final YTDatabaseSession session = youTrackDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    final YouTrackDBConfig wrongKeyOneYouTrackDBConfig =
        YouTrackDBConfig.builder()
            .addConfig(
                GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8=")
            .build();
    try (final YouTrackDB youTrackDB =
        new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), wrongKeyOneYouTrackDBConfig)) {
      try {
        try (final YTDatabaseSession session = youTrackDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    final YouTrackDBConfig wrongKeyTwoYouTrackDBConfig =
        YouTrackDBConfig.builder()
            .addConfig(
                GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8")
            .build();
    try (final YouTrackDB youTrackDB =
        new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), wrongKeyTwoYouTrackDBConfig)) {
      try {
        try (final YTDatabaseSession session = youTrackDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    try (final YouTrackDB youTrackDB =
        new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()), youTrackDBConfig)) {
      try (final YTDatabaseSessionInternal session =
          (YTDatabaseSessionInternal) youTrackDB.open("encryption", "admin", "admin")) {
        final OIndexManagerAbstract indexManager = session.getMetadata().getIndexManagerInternal();
        final OIndex treeIndex = indexManager.getIndex(session, "EncryptedTree");
        final OIndex hashIndex = indexManager.getIndex(session, "EncryptedHash");

        for (final EntityImpl document : session.browseClass("EncryptedData")) {
          final int id = document.getProperty("id");
          final YTRID treeRid;
          try (Stream<YTRID> rids = treeIndex.getInternal().getRids(session, id)) {
            treeRid = rids.findFirst().orElse(null);
          }
          final YTRID hashRid;
          try (Stream<YTRID> rids = hashIndex.getInternal().getRids(session, id)) {
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
        "./target/databases" + File.separator + OStorageEncryptionTestIT.class.getSimpleName();
    final File dbDirectoryFile = new File(dbDirectory);
    OFileUtils.deleteRecursively(dbDirectoryFile);
    return dbDirectoryFile;
  }

  @Test
  public void testEncryptionSingleDatabase() {
    final File dbDirectoryFile = cleanAndGetDirectory();

    try (final YouTrackDB youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()), YouTrackDBConfig.defaultConfig())) {
      final YouTrackDBConfig youTrackDBConfig =
          YouTrackDBConfig.builder()
              .addConfig(GlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
              .build();

      youTrackDB.execute(
          "create database encryption plocal users ( admin identified by 'admin' role admin)");
    }
    try (final YouTrackDB youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()), YouTrackDBConfig.defaultConfig())) {
      final YouTrackDBConfig youTrackDBConfig =
          YouTrackDBConfig.builder()
              .addConfig(GlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
              .build();
      try (var session =
          (YTDatabaseSessionInternal) youTrackDB.open("encryption", "admin", "admin",
              youTrackDBConfig)) {
        final YTSchema schema = session.getMetadata().getSchema();
        final YTClass cls = schema.createClass("EncryptedData");

        final EntityImpl document = new EntityImpl(cls);
        document.setProperty("id", 10);
        document.setProperty(
            "value",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor"
                + " incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis"
                + " nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
                + " ");
        document.save();

        try (YTResultSet resultSet = session.query("select from EncryptedData where id = ?", 10)) {
          assertTrue(resultSet.hasNext());
        }
      }
    }
  }
}
