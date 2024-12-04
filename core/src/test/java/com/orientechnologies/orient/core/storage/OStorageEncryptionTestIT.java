package com.orientechnologies.orient.core.storage;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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

    final OxygenDBConfig oxygenDBConfig =
        OxygenDBConfig.builder()
            .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
            .build();
    try (final OxygenDB oxygenDB =
        new OxygenDB(DBTestBase.embeddedDBUrl(getClass()), oxygenDBConfig)) {
      oxygenDB.execute(
          "create database encryption plocal users ( admin identified by 'admin' role admin)");
      try (var session = (ODatabaseSessionInternal) oxygenDB.open("encryption", "admin", "admin")) {
        final OSchema schema = session.getMetadata().getSchema();
        final OClass cls = schema.createClass("EncryptedData");
        cls.createProperty(session, "id", OType.INTEGER);
        cls.createProperty(session, "value", OType.STRING);

        cls.createIndex(session, "EncryptedTree", OClass.INDEX_TYPE.UNIQUE, "id");
        cls.createIndex(session, "EncryptedHash", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "id");

        for (int i = 0; i < 10_000; i++) {
          final ODocument document = new ODocument(cls);
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
          try (OResultSet resultSet =
              session.query("select from EncryptedData where id = ?", random.nextInt(10_000_000))) {
            if (resultSet.hasNext()) {
              final OResult result = resultSet.next();
              result.getElement().ifPresent(ORecord::delete);
            }
          }
        }
      }
    }

    try (final OxygenDB oxygenDB =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()), OxygenDBConfig.defaultConfig())) {
      try {
        try (final ODatabaseSession session = oxygenDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    final OxygenDBConfig wrongKeyOneOxygenDBConfig =
        OxygenDBConfig.builder()
            .addConfig(
                OGlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8=")
            .build();
    try (final OxygenDB oxygenDB =
        new OxygenDB(DBTestBase.embeddedDBUrl(getClass()), wrongKeyOneOxygenDBConfig)) {
      try {
        try (final ODatabaseSession session = oxygenDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    final OxygenDBConfig wrongKeyTwoOxygenDBConfig =
        OxygenDBConfig.builder()
            .addConfig(
                OGlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                "DD0ViGecppQOx4ijWL4XGBwun9NAfbqFaDnVpn9+lj8")
            .build();
    try (final OxygenDB oxygenDB =
        new OxygenDB(DBTestBase.embeddedDBUrl(getClass()), wrongKeyTwoOxygenDBConfig)) {
      try {
        try (final ODatabaseSession session = oxygenDB.open("encryption", "admin", "admin")) {
          Assert.fail();
        }
      } catch (Exception e) {
        // ignore
      }
    }

    try (final OxygenDB oxygenDB =
        new OxygenDB(DBTestBase.embeddedDBUrl(getClass()), oxygenDBConfig)) {
      try (final ODatabaseSessionInternal session =
          (ODatabaseSessionInternal) oxygenDB.open("encryption", "admin", "admin")) {
        final OIndexManagerAbstract indexManager = session.getMetadata().getIndexManagerInternal();
        final OIndex treeIndex = indexManager.getIndex(session, "EncryptedTree");
        final OIndex hashIndex = indexManager.getIndex(session, "EncryptedHash");

        for (final ODocument document : session.browseClass("EncryptedData")) {
          final int id = document.getProperty("id");
          final ORID treeRid;
          try (Stream<ORID> rids = treeIndex.getInternal().getRids(session, id)) {
            treeRid = rids.findFirst().orElse(null);
          }
          final ORID hashRid;
          try (Stream<ORID> rids = hashIndex.getInternal().getRids(session, id)) {
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

    try (final OxygenDB oxygenDB =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()), OxygenDBConfig.defaultConfig())) {
      final OxygenDBConfig oxygenDBConfig =
          OxygenDBConfig.builder()
              .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
              .build();

      oxygenDB.execute(
          "create database encryption plocal users ( admin identified by 'admin' role admin)");
    }
    try (final OxygenDB oxygenDB =
        new OxygenDB(
            DBTestBase.embeddedDBUrl(getClass()), OxygenDBConfig.defaultConfig())) {
      final OxygenDBConfig oxygenDBConfig =
          OxygenDBConfig.builder()
              .addConfig(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY, "T1JJRU5UREJfSVNfQ09PTA==")
              .build();
      try (var session =
          (ODatabaseSessionInternal) oxygenDB.open("encryption", "admin", "admin",
              oxygenDBConfig)) {
        final OSchema schema = session.getMetadata().getSchema();
        final OClass cls = schema.createClass("EncryptedData");

        final ODocument document = new ODocument(cls);
        document.setProperty("id", 10);
        document.setProperty(
            "value",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor"
                + " incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis"
                + " nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat."
                + " ");
        document.save();

        try (OResultSet resultSet = session.query("select from EncryptedData where id = ?", 10)) {
          assertTrue(resultSet.hasNext());
        }
      }
    }
  }
}
