package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseCompare;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @since 29.05.13
 */
public class LocalPaginatedStorageRestoreFromWALIT {

  private static File buildDir;
  private DatabaseSessionInternal testDocumentTx;
  private DatabaseSessionInternal baseDocumentTx;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private static void copyFile(String from, String to) throws IOException {
    final File fromFile = new File(from);
    FileInputStream fromInputStream = new FileInputStream(fromFile);
    BufferedInputStream fromBufferedStream = new BufferedInputStream(fromInputStream);

    FileOutputStream toOutputStream = new FileOutputStream(to);
    byte[] data = new byte[1024];
    int bytesRead = fromBufferedStream.read(data);
    while (bytesRead > 0) {
      toOutputStream.write(data, 0, bytesRead);
      bytesRead = fromBufferedStream.read(data);
    }

    fromBufferedStream.close();
    toOutputStream.close();
  }

  @BeforeClass
  public static void beforeClass() {
    GlobalConfiguration.STORAGE_COMPRESSION_METHOD.setValue("nothing");
    GlobalConfiguration.FILE_LOCK.setValue(false);
    GlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(100000000);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageRestoreFromWAL";

    buildDir = new File(buildDirectory);
    if (buildDir.exists()) {
      buildDir.delete();
    }

    buildDir.mkdir();
  }

  @AfterClass
  public static void afterClass() {
    buildDir.delete();
  }

  @Before
  public void beforeMethod() {
    baseDocumentTx =
        new DatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageRestoreFromWAL");
    if (baseDocumentTx.exists()) {
      baseDocumentTx.open("admin", "admin");
      baseDocumentTx.drop();
    }

    baseDocumentTx.create();

    createSchema(baseDocumentTx);
  }

  @After
  public void afterMethod() {
    testDocumentTx.open("admin", "admin");
    testDocumentTx.drop();

    baseDocumentTx.open("admin", "admin");
    baseDocumentTx.drop();
  }

  @Test
  public void testSimpleRestore() throws Exception {
    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    for (int i = 0; i < 8; i++) {
      futures.add(executorService.submit(new DataPropagationTask()));
    }

    for (Future<Void> future : futures) {
      future.get();
    }

    Thread.sleep(1500);
    copyDataFromTestWithoutClose();
    Storage baseStorage = baseDocumentTx.getStorage();
    baseDocumentTx.close();
    baseStorage.close(baseDocumentTx);

    testDocumentTx =
        new DatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageRestoreFromWAL");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    testDocumentTx =
        new DatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageRestoreFromWAL");
    testDocumentTx.open("admin", "admin");
    baseDocumentTx =
        new DatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageRestoreFromWAL");
    baseDocumentTx.open("admin", "admin");
    DatabaseCompare databaseCompare =
        new DatabaseCompare(testDocumentTx, baseDocumentTx, System.out::println);
    databaseCompare.setCompareIndexMetadata(true);

    Assert.assertTrue(databaseCompare.compare());
    testDocumentTx.close();
    baseDocumentTx.close();
  }

  private void copyDataFromTestWithoutClose() throws Exception {
    final String testStoragePath = baseDocumentTx.getURL().substring("plocal:".length());
    final String copyTo =
        buildDir.getAbsolutePath() + File.separator + "testLocalPaginatedStorageRestoreFromWAL";
    FileUtils.deleteRecursively(new File(copyTo));

    final File testStorageDir = new File(testStoragePath);
    final File copyToDir = new File(copyTo);

    Assert.assertFalse(copyToDir.exists());
    Assert.assertTrue(copyToDir.mkdir());

    File[] storageFiles = testStorageDir.listFiles();
    Assert.assertNotNull(storageFiles);

    for (File storageFile : storageFiles) {
      String copyToPath;
      if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.wmr")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.wmr";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.0.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.0.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.1.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.1.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.2.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.2.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.3.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.3.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.4.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.4.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.5.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.5.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.6.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.6.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.7.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.7.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.8.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.8.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.9.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.9.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.10.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.10.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.11.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.11.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.12.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.12.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.13.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.13.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.14.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.14.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.15.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.15.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.16.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.16.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.17.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.17.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.18.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.18.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.19.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.19.wal";
      } else if (storageFile.getName().equals("baseLocalPaginatedStorageRestoreFromWAL.20.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromWAL.20.wal";
      } else {
        copyToPath = copyToDir.getAbsolutePath() + File.separator + storageFile.getName();
      }

      if (storageFile.getName().equals("dirty.fl")) {
        continue;
      }

      copyFile(storageFile.getAbsolutePath(), copyToPath);
    }
  }

  private void createSchema(DatabaseSessionInternal databaseDocumentTx) {
    DatabaseRecordThreadLocal.instance().set(databaseDocumentTx);

    Schema schema = databaseDocumentTx.getMetadata().getSchema();
    SchemaClass testOneClass = schema.createClass("TestOne");

    testOneClass.createProperty(databaseDocumentTx, "intProp", PropertyType.INTEGER);
    testOneClass.createProperty(databaseDocumentTx, "stringProp", PropertyType.STRING);
    testOneClass.createProperty(databaseDocumentTx, "stringSet", PropertyType.EMBEDDEDSET,
        PropertyType.STRING);
    testOneClass.createProperty(databaseDocumentTx, "linkMap", PropertyType.LINKMAP);

    SchemaClass testTwoClass = schema.createClass("TestTwo");

    testTwoClass.createProperty(databaseDocumentTx, "stringList", PropertyType.EMBEDDEDLIST,
        PropertyType.STRING);
  }

  public class DataPropagationTask implements Callable<Void> {

    @Override
    public Void call() throws Exception {

      Random random = new Random();

      final DatabaseSessionInternal db = new DatabaseDocumentTx(baseDocumentTx.getURL());
      db.open("admin", "admin");
      try {
        List<RID> testTwoList = new ArrayList<RID>();
        List<RID> firstDocs = new ArrayList<RID>();

        SchemaClass classOne = db.getMetadata().getSchema().getClass("TestOne");
        SchemaClass classTwo = db.getMetadata().getSchema().getClass("TestTwo");

        for (int i = 0; i < 5000; i++) {
          EntityImpl docOne = ((EntityImpl) db.newEntity(classOne));
          docOne.field("intProp", random.nextInt());

          byte[] stringData = new byte[256];
          random.nextBytes(stringData);
          String stringProp = new String(stringData);

          docOne.field("stringProp", stringProp);

          Set<String> stringSet = new HashSet<String>();
          for (int n = 0; n < 5; n++) {
            stringSet.add("str" + random.nextInt());
          }
          docOne.field("stringSet", stringSet);

          docOne.save();

          firstDocs.add(docOne.getIdentity());

          if (random.nextBoolean()) {
            EntityImpl docTwo = ((EntityImpl) db.newEntity(classTwo));

            List<String> stringList = new ArrayList<String>();

            for (int n = 0; n < 5; n++) {
              stringList.add("strnd" + random.nextInt());
            }

            docTwo.field("stringList", stringList);
            docTwo.save();

            testTwoList.add(docTwo.getIdentity());
          }

          if (!testTwoList.isEmpty()) {
            int startIndex = random.nextInt(testTwoList.size());
            int endIndex = random.nextInt(testTwoList.size() - startIndex) + startIndex;

            Map<String, RID> linkMap = new HashMap<String, RID>();

            for (int n = startIndex; n < endIndex; n++) {
              RID docTwoRid = testTwoList.get(n);
              linkMap.put(docTwoRid.toString(), docTwoRid);
            }

            docOne.field("linkMap", linkMap);
            docOne.save();
          }

          boolean deleteDoc = random.nextDouble() <= 0.2;
          if (deleteDoc) {
            RID rid = firstDocs.remove(random.nextInt(firstDocs.size()));
            db.delete(rid);
          }
        }
      } finally {
        db.close();
      }

      return null;
    }
  }
}
