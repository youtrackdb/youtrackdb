package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseCompare;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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
import org.junit.Ignore;
import org.junit.Test;

/**
 * @since 18.06.13
 */
public class LocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords {

  private static File buildDir;
  private DatabaseSessionInternal testDocumentTx;
  private DatabaseSessionInternal baseDocumentTx;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @BeforeClass
  public static void beforeClass() {
    GlobalConfiguration.STORAGE_COMPRESSION_METHOD.setValue("nothing");
    GlobalConfiguration.FILE_LOCK.setValue(false);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/localPaginatedStorageRestoreFromWALAndAddAdditionalRecords";

    buildDir = new File(buildDirectory);
    if (buildDir.exists()) {
      buildDir.delete();
    }

    buildDir.mkdir();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    //    Files.delete(buildDir.toPath());
    FileUtils.deleteRecursively(buildDir);
  }

  @Before
  public void beforeMethod() throws IOException {
    FileUtils.deleteRecursively(buildDir);

    baseDocumentTx =
        new DatabaseDocumentTx(
            "plocal:"
                + buildDir.getAbsolutePath()
                + "/baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords");
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
  @Ignore
  public void testRestoreAndAddNewItems() throws Exception {
    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    Random random = new Random();

    long[] seeds = new long[5];
    for (int i = 0; i < 5; i++) {
      seeds[i] = random.nextLong();
      System.out.println("Seed [" + i + "] = " + seeds[i]);
    }

    for (long seed : seeds) {
      futures.add(executorService.submit(new DataPropagationTask(seed)));
    }

    for (Future<Void> future : futures) {
      future.get();
    }

    futures.clear();

    Thread.sleep(1500);
    copyDataFromTestWithoutClose();
    Storage storage = baseDocumentTx.getStorage();
    baseDocumentTx.close();
    storage.close(baseDocumentTx);

    testDocumentTx =
        new DatabaseDocumentTx(
            "plocal:"
                + buildDir.getAbsolutePath()
                + "/testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords");
    testDocumentTx.open("admin", "admin");
    testDocumentTx.close();

    long dataAddSeed = random.nextLong();
    System.out.println("Data add seed = " + dataAddSeed);
    for (int i = 0; i < 1; i++) {
      futures.add(executorService.submit(new DataPropagationTask(dataAddSeed)));
    }

    for (Future<Void> future : futures) {
      future.get();
    }

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
    final Path testStoragePath = Paths.get(baseDocumentTx.getURL().substring("plocal:".length()));
    Path buildPath = Paths.get(buildDir.toURI());

    final Path copyTo =
        buildPath.resolve("testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords");

    Files.copy(testStoragePath, copyTo);

    Files.walkFileTree(
        testStoragePath,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Path fileToCopy = copyTo.resolve(testStoragePath.relativize(file));
            if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.wmr")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.wmr");
            } else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.0.wal")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.0.wal");
            } else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.1.wal")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.1.wal");
            } else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.2.wal")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.2.wal");
            } else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.3.wal")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.3.wal");
            } else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.4.wal")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.4.wal");
            } else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.5.wal")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.5.wal");
            } else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.6.wal")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.6.wal");
            } else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.7.wal")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.7.wal");
            } else if (fileToCopy.endsWith(
                "baseLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.8.wal")) {
              fileToCopy =
                  fileToCopy
                      .getParent()
                      .resolve(
                          "testLocalPaginatedStorageRestoreFromWALAndAddAdditionalRecords.8.wal");
            }

            if (fileToCopy.endsWith("dirty.fl")) {
              return FileVisitResult.CONTINUE;
            }

            Files.copy(file, fileToCopy);

            return FileVisitResult.CONTINUE;
          }
        });
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

    private final DatabaseSessionInternal baseDB;
    private DatabaseSessionInternal testDB;
    private final long seed;

    public DataPropagationTask(long seed) {
      this.seed = seed;

      baseDB = new DatabaseDocumentTx(baseDocumentTx.getURL());
      baseDB.open("admin", "admin");

      if (testDocumentTx != null) {
        testDB = new DatabaseDocumentTx(testDocumentTx.getURL());
        testDB.open("admin", "admin");
      }
    }

    @Override
    public Void call() throws Exception {

      Random random = new Random(seed);

      DatabaseRecordThreadLocal.instance().set(baseDB);

      try {
        List<RID> testTwoList = new ArrayList<RID>();
        List<RID> firstDocs = new ArrayList<RID>();

        SchemaClass classOne = baseDB.getMetadata().getSchema().getClass("TestOne");
        SchemaClass classTwo = baseDB.getMetadata().getSchema().getClass("TestTwo");

        for (int i = 0; i < 10000; i++) {
          EntityImpl docOne = ((EntityImpl) baseDB.newEntity(classOne));
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

          saveDoc(docOne);

          firstDocs.add(docOne.getIdentity());

          if (random.nextBoolean()) {
            EntityImpl docTwo = ((EntityImpl) baseDB.newEntity(classTwo));

            List<String> stringList = new ArrayList<String>();

            for (int n = 0; n < 5; n++) {
              stringList.add("strnd" + random.nextInt());
            }

            docTwo.field("stringList", stringList);
            saveDoc(docTwo);
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

            saveDoc(docOne);
          }

          boolean deleteDoc = random.nextDouble() <= 0.2;
          if (deleteDoc) {
            RID rid = firstDocs.remove(random.nextInt(firstDocs.size()));

            deleteDoc(rid);
          }
        }
      } finally {
        baseDB.close();
        if (testDB != null) {
          testDB.close();
        }
      }

      return null;
    }

    private void saveDoc(EntityImpl document) {
      DatabaseRecordThreadLocal.instance().set(baseDB);

      EntityImpl testDoc = ((EntityImpl) baseDB.newEntity());
      document.copyTo(testDoc);
      document.save();

      if (testDB != null) {
        DatabaseRecordThreadLocal.instance().set(testDB);
        testDoc.save();

        Assert.assertEquals(testDoc.getIdentity(), document.getIdentity());

        DatabaseRecordThreadLocal.instance().set(baseDB);
      }
    }

    private void deleteDoc(RID rid) {
      baseDB.delete(rid);

      if (testDB != null) {
        DatabaseRecordThreadLocal.instance().set(testDB);
        Assert.assertNotNull(testDB.load(rid));
        testDB.delete(rid);
        Assert.assertNull(testDB.load(rid));
        DatabaseRecordThreadLocal.instance().set(baseDB);
      }
    }
  }
}
