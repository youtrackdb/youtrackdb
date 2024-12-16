package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseCompare;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @since 14.06.13
 */
public class LocalPaginatedStorageRestoreTx {

  private DatabaseSessionInternal testDocumentTx;
  private DatabaseSessionInternal baseDocumentTx;
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
    GlobalConfiguration.FILE_LOCK.setValue(false);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageRestoreFromTx";

    buildDir = new File(buildDirectory);
    if (buildDir.exists()) {
      buildDir.delete();
    }

    buildDir.mkdir();

    baseDocumentTx =
        new DatabaseDocumentTx(
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
    Storage storage = baseDocumentTx.getStorage();
    baseDocumentTx.close();
    storage.close(baseDocumentTx);

    testDocumentTx =
        new DatabaseDocumentTx(
            "plocal:" + buildDir.getAbsolutePath() + "/testLocalPaginatedStorageRestoreFromTx");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    DatabaseCompare databaseCompare =
        new DatabaseCompare(
            testDocumentTx,
            baseDocumentTx,
            new CommandOutputListener() {
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
      int rollbacksCount = 0;
      try {
        List<RID> secondDocs = new ArrayList<RID>();
        List<RID> firstDocs = new ArrayList<RID>();

        SchemaClass classOne = db.getMetadata().getSchema().getClass("TestOne");
        SchemaClass classTwo = db.getMetadata().getSchema().getClass("TestTwo");

        for (int i = 0; i < 20000; i++) {
          try {
            db.begin();

            EntityImpl docOne = new EntityImpl(classOne);
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

            EntityImpl docTwo = null;

            if (random.nextBoolean()) {
              docTwo = new EntityImpl(classTwo);

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

              Map<String, RID> linkMap = new HashMap<String, RID>();

              for (int n = startIndex; n < endIndex; n++) {
                RID docTwoRid = secondDocs.get(n);
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
                  RID rid = firstDocs.get(deleteIndex);
                  db.delete(rid);
                }
              }
            }

            if (!secondDocs.isEmpty() && (random.nextDouble() <= 0.2)) {
              EntityImpl conflictDocTwo = new EntityImpl();
              RecordInternal.setIdentity(conflictDocTwo, new RecordId(secondDocs.get(0)));
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
