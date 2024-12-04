package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseCompare;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.storage.OStorage;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @since 14.06.13
 */
public class LocalPaginatedStorageRestoreTx {

  private YTDatabaseSessionInternal testDocumentTx;
  private YTDatabaseSessionInternal baseDocumentTx;
  private File buildDir;

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

  @Before
  public void beforeClass() {
    YTGlobalConfiguration.FILE_LOCK.setValue(false);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageRestoreFromTx";

    buildDir = new File(buildDirectory);
    if (buildDir.exists()) {
      buildDir.delete();
    }

    buildDir.mkdir();

    baseDocumentTx =
        new YTDatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/baseLocalPaginatedStorageRestoreFromTx");
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

    buildDir.delete();
  }

  @Test
  @Ignore
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
    OStorage storage = baseDocumentTx.getStorage();
    baseDocumentTx.close();
    storage.close(baseDocumentTx);

    testDocumentTx =
        new YTDatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageRestoreFromTx");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    ODatabaseCompare databaseCompare =
        new ODatabaseCompare(
            testDocumentTx,
            baseDocumentTx,
            new OCommandOutputListener() {
              @Override
              public void onMessage(String text) {
                System.out.println(text);
              }
            });
    databaseCompare.setCompareIndexMetadata(true);

    Assert.assertTrue(databaseCompare.compare());
  }

  private void copyDataFromTestWithoutClose() throws Exception {
    final String testStoragePath = baseDocumentTx.getURL().substring("plocal:".length());
    final String copyTo =
        buildDir.getAbsolutePath() + File.separator + "testLocalPaginatedStorageRestoreFromTx";

    final File testStorageDir = new File(testStoragePath);
    final File copyToDir = new File(copyTo);

    Assert.assertFalse(copyToDir.exists());
    Assert.assertTrue(copyToDir.mkdir());

    File[] storageFiles = testStorageDir.listFiles();
    Assert.assertNotNull(storageFiles);

    for (File storageFile : storageFiles) {
      String copyToPath;
      if (storageFile.getAbsolutePath().endsWith("baseLocalPaginatedStorageRestoreFromTx.wmr")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.wmr";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.0.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.0.wal";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.1.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.1.wal";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.2.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.2.wal";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.3.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.3.wal";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.4.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.4.wal";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.5.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.5.wal";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.6.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.6.wal";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.7.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.7.wal";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.8.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.8.wal";
      } else if (storageFile
          .getAbsolutePath()
          .endsWith("baseLocalPaginatedStorageRestoreFromTx.9.wal")) {
        copyToPath =
            copyToDir.getAbsolutePath()
                + File.separator
                + "testLocalPaginatedStorageRestoreFromTx.9.wal";
      } else {
        copyToPath = copyToDir.getAbsolutePath() + File.separator + storageFile.getName();
      }

      if (storageFile.getName().equals("dirty.fl")) {
        continue;
      }

      copyFile(storageFile.getAbsolutePath(), copyToPath);
    }
  }

  private void createSchema(YTDatabaseSessionInternal databaseDocumentTx) {
    ODatabaseRecordThreadLocal.instance().set(databaseDocumentTx);

    YTSchema schema = databaseDocumentTx.getMetadata().getSchema();
    YTClass testOneClass = schema.createClass("TestOne");

    testOneClass.createProperty(databaseDocumentTx, "intProp", YTType.INTEGER);
    testOneClass.createProperty(databaseDocumentTx, "stringProp", YTType.STRING);
    testOneClass.createProperty(databaseDocumentTx, "stringSet", YTType.EMBEDDEDSET, YTType.STRING);
    testOneClass.createProperty(databaseDocumentTx, "linkMap", YTType.LINKMAP);

    YTClass testTwoClass = schema.createClass("TestTwo");

    testTwoClass.createProperty(databaseDocumentTx, "stringList", YTType.EMBEDDEDLIST,
        YTType.STRING);
  }

  public class DataPropagationTask implements Callable<Void> {

    @Override
    public Void call() throws Exception {

      Random random = new Random();

      final YTDatabaseSessionInternal db = new YTDatabaseDocumentTx(baseDocumentTx.getURL());
      db.open("admin", "admin");
      int rollbacksCount = 0;
      try {
        List<YTRID> secondDocs = new ArrayList<YTRID>();
        List<YTRID> firstDocs = new ArrayList<YTRID>();

        YTClass classOne = db.getMetadata().getSchema().getClass("TestOne");
        YTClass classTwo = db.getMetadata().getSchema().getClass("TestTwo");

        for (int i = 0; i < 20000; i++) {
          try {
            db.begin();

            YTDocument docOne = new YTDocument(classOne);
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

            YTDocument docTwo = null;

            if (random.nextBoolean()) {
              docTwo = new YTDocument(classTwo);

              List<String> stringList = new ArrayList<String>();

              for (int n = 0; n < 5; n++) {
                stringList.add("strnd" + random.nextInt());
              }

              docTwo.field("stringList", stringList);
              docTwo.save();
            }

            if (!secondDocs.isEmpty()) {
              int startIndex = random.nextInt(secondDocs.size());
              int endIndex = random.nextInt(secondDocs.size() - startIndex) + startIndex;

              Map<String, YTRID> linkMap = new HashMap<String, YTRID>();

              for (int n = startIndex; n < endIndex; n++) {
                YTRID docTwoRid = secondDocs.get(n);
                linkMap.put(docTwoRid.toString(), docTwoRid);
              }

              docOne.field("linkMap", linkMap);
              docOne.save();
            }

            int deleteIndex = -1;
            if (!firstDocs.isEmpty()) {
              boolean deleteDoc = random.nextDouble() <= 0.2;

              if (deleteDoc) {
                deleteIndex = random.nextInt(firstDocs.size());
                if (deleteIndex >= 0) {
                  YTRID rid = firstDocs.get(deleteIndex);
                  db.delete(rid);
                }
              }
            }

            if (!secondDocs.isEmpty() && (random.nextDouble() <= 0.2)) {
              YTDocument conflictDocTwo = new YTDocument();
              ORecordInternal.setIdentity(conflictDocTwo, new YTRecordId(secondDocs.get(0)));
              conflictDocTwo.setDirty();
              conflictDocTwo.save();
            }

            db.commit();

            if (deleteIndex >= 0) {
              firstDocs.remove(deleteIndex);
            }

            firstDocs.add(docOne.getIdentity());
            if (docTwo != null) {
              secondDocs.add(docTwo.getIdentity());
            }

          } catch (Exception e) {
            db.rollback();
            rollbacksCount++;
          }
        }
      } finally {
        db.close();
      }

      System.out.println("Rollbacks count " + rollbacksCount);
      return null;
    }
  }
}
