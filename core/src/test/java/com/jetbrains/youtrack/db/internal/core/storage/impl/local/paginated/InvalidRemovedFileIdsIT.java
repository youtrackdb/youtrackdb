package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.StringSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.cache.WriteCache;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class InvalidRemovedFileIdsIT {

  @Test
  public void testRemovedFileIds() throws Exception {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    final String dbName = InvalidRemovedFileIdsIT.class.getSimpleName();
    final String dbPath = buildDirectory + File.separator + dbName;

    deleteDirectory(new File(dbPath));

    final YouTrackDBConfig config =
        YouTrackDBConfig.builder()
            .addAttribute(DatabaseSession.ATTRIBUTES.MINIMUM_CLUSTERS, 1)
            .build();

    YouTrackDB youTrackDB = new YouTrackDBImpl("plocal:" + buildDirectory, config);
    youTrackDB.execute(
        "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");
    var db = (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    Storage storage = db.getStorage();
    WriteCache writeCache = ((AbstractPaginatedStorage) storage).getWriteCache();
    Map<String, Long> files = writeCache.files();

    Map<String, Integer> filesWithIntIds = new HashMap<>();

    for (Map.Entry<String, Long> file : files.entrySet()) {
      filesWithIntIds.put(file.getKey(), writeCache.internalFileId(file.getValue()));
    }

    db.close();
    youTrackDB.close();

    // create file map of v1 binary format because but with incorrect negative file ids is present
    // only there
    final RandomAccessFile fileMap = new RandomAccessFile(new File(dbPath, "name_id_map.cm"), "rw");
    // write all existing files so map will be regenerated on open
    for (Map.Entry<String, Integer> entry : filesWithIntIds.entrySet()) {
      writeNameIdEntry(fileMap, entry.getKey(), entry.getValue());
    }

    writeNameIdEntry(fileMap, "c1.cpm", -100);
    writeNameIdEntry(fileMap, "c1.pcl", -100);

    writeNameIdEntry(fileMap, "c2.cpm", -200);
    writeNameIdEntry(fileMap, "c2.pcl", -200);
    writeNameIdEntry(fileMap, "c2.pcl", -400);

    writeNameIdEntry(fileMap, "c3.cpm", -500);
    writeNameIdEntry(fileMap, "c3.pcl", -500);
    writeNameIdEntry(fileMap, "c4.cpm", -500);
    writeNameIdEntry(fileMap, "c4.pcl", -600);
    writeNameIdEntry(fileMap, "c4.cpm", -600);

    fileMap.close();

    youTrackDB = new YouTrackDBImpl("plocal:" + buildDirectory, config);
    db = (DatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    final Schema schema = db.getMetadata().getSchema();
    schema.createClass("c1");
    schema.createClass("c2");
    schema.createClass("c3");
    schema.createClass("c4");

    storage = db.getStorage();
    writeCache = ((AbstractPaginatedStorage) storage).getWriteCache();

    files = writeCache.files();
    final Set<Long> ids = new HashSet<>();

    final Long c1_cpm_id = files.get("c1.cpm");
    Assert.assertNotNull(c1_cpm_id);
    Assert.assertTrue(c1_cpm_id > 0);
    Assert.assertTrue(ids.add(c1_cpm_id));

    final Long c1_pcl_id = files.get("c1.pcl");
    Assert.assertNotNull(c1_pcl_id);
    Assert.assertTrue(c1_pcl_id > 0);
    Assert.assertTrue(ids.add(c1_pcl_id));

    final Long c2_cpm_id = files.get("c2.cpm");
    Assert.assertNotNull(c2_cpm_id);
    Assert.assertTrue(ids.add(c2_cpm_id));
    Assert.assertEquals(
        200, writeCache.internalFileId(c2_cpm_id)); // check that updated file map has been read

    final Long c2_pcl_id = files.get("c2.pcl");
    Assert.assertNotNull(c2_pcl_id);
    Assert.assertTrue(ids.add(c2_pcl_id));
    Assert.assertEquals(
        400, writeCache.internalFileId(c2_pcl_id)); // check that updated file map has been read

    final Long c3_cpm_id = files.get("c3.cpm");
    Assert.assertNotNull(c3_cpm_id);
    Assert.assertTrue(c3_cpm_id > 0);
    Assert.assertTrue(ids.add(c3_cpm_id));

    final Long c3_pcl_id = files.get("c3.pcl");
    Assert.assertNotNull(c3_pcl_id);
    Assert.assertTrue(c3_pcl_id > 0);
    Assert.assertTrue(ids.add(c3_pcl_id));

    final Long c4_cpm_id = files.get("c4.cpm");
    Assert.assertNotNull(c4_cpm_id);
    Assert.assertTrue(c4_cpm_id > 0);
    Assert.assertTrue(ids.add(c4_cpm_id));

    final Long c4_pcl_id = files.get("c4.pcl");
    Assert.assertNotNull(c4_pcl_id);
    Assert.assertTrue(c1_pcl_id > 0);
    Assert.assertTrue(ids.add(c4_pcl_id));

    db.close();
    youTrackDB.close();
  }

  private static void writeNameIdEntry(RandomAccessFile file, String name, int fileId)
      throws IOException {
    final int nameSize = StringSerializer.INSTANCE.getObjectSize(name);

    byte[] serializedRecord =
        new byte[IntegerSerializer.INT_SIZE + nameSize + LongSerializer.LONG_SIZE];
    IntegerSerializer.INSTANCE.serializeLiteral(nameSize, serializedRecord, 0);
    StringSerializer.INSTANCE.serialize(name, serializedRecord, IntegerSerializer.INT_SIZE);
    LongSerializer.INSTANCE.serializeLiteral(
        fileId, serializedRecord, IntegerSerializer.INT_SIZE + nameSize);

    file.write(serializedRecord);
  }

  private static void deleteDirectory(final File directory) {
    if (directory.exists()) {
      final File[] files = directory.listFiles();

      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            Assert.assertTrue(file.delete());
          }
        }

        Assert.assertTrue(directory.delete());
      }
    }
  }
}
