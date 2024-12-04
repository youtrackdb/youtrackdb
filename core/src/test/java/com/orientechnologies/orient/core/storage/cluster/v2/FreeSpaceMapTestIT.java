package com.orientechnologies.orient.core.storage.cluster.v2;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FreeSpaceMapTestIT {

  protected FreeSpaceMap freeSpaceMap;

  protected static YouTrackDB youTrackDB;
  protected static String dbName;
  protected static OAbstractPaginatedStorage storage;
  private static OAtomicOperationsManager atomicOperationsManager;

  @BeforeClass
  public static void beforeClass() throws IOException {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty()) {
      buildDirectory = ".";
    }

    buildDirectory += File.separator + FreeSpaceMapTestIT.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(buildDirectory));

    dbName = "freeSpaceMapTest";

    youTrackDB = new YouTrackDB("plocal:" + buildDirectory, YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");

    final YTDatabaseSessionInternal databaseDocumentTx =
        (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    storage = (OAbstractPaginatedStorage) databaseDocumentTx.getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();
    databaseDocumentTx.close();
  }

  @AfterClass
  public static void afterClass() {
    youTrackDB.drop(dbName);
    youTrackDB.close();
  }

  @Before
  public void before() throws IOException {
    freeSpaceMap = new FreeSpaceMap(storage, "freeSpaceMap", ".fsm", "freeSpaceMap");

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation -> {
          freeSpaceMap.create(atomicOperation);
        });
  }

  @Test
  public void findSinglePage() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        operation -> {
          freeSpaceMap.updatePageFreeSpace(operation, 3, 512);
          Assert.assertEquals(3, freeSpaceMap.findFreePage(259));
        });
  }

  @Test
  public void findSinglePageHighIndex() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        operation -> {
          freeSpaceMap.updatePageFreeSpace(operation, 128956, 512);
          Assert.assertEquals(128956, freeSpaceMap.findFreePage(259));
        });
  }

  @Test
  public void findSinglePageLowerSpaceOne() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        operation -> {
          freeSpaceMap.updatePageFreeSpace(operation, 3, 1024);
          freeSpaceMap.updatePageFreeSpace(operation, 4, 2029);
          freeSpaceMap.updatePageFreeSpace(operation, 5, 3029);

          Assert.assertEquals(4, freeSpaceMap.findFreePage(1024));
        });
  }

  @Test
  public void findSinglePageLowerSpaceTwo() throws IOException {
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        operation -> {
          freeSpaceMap.updatePageFreeSpace(operation, 3, 1024);
          freeSpaceMap.updatePageFreeSpace(operation, 4, 2029);
          freeSpaceMap.updatePageFreeSpace(operation, 5, 3029);

          Assert.assertEquals(5, freeSpaceMap.findFreePage(2050));
        });
  }

  @Test
  public void randomPages() throws IOException {
    final int pages = 1_000;
    final int checks = 1_000;

    final HashMap<Integer, Integer> pageSpaceMap = new HashMap<>();
    final long seed = 1107466507161549L; // System.nanoTime();
    System.out.println("randomPages seed - " + seed);
    final Random random = new Random(seed);

    int[] maxFreeSpaceIndex = new int[]{-1};
    for (int i = 0; i < pages; i++) {
      final int pageIndex = i;

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          operation -> {
            final int freeSpace = random.nextInt(ODurablePage.MAX_PAGE_SIZE_BYTES);
            final int freeSpaceIndex =
                (freeSpace - FreeSpaceMap.NORMALIZATION_INTERVAL + 1)
                    / FreeSpaceMap.NORMALIZATION_INTERVAL;
            if (maxFreeSpaceIndex[0] < freeSpaceIndex) {
              maxFreeSpaceIndex[0] = freeSpaceIndex;
            }

            pageSpaceMap.put(pageIndex, freeSpace);
            freeSpaceMap.updatePageFreeSpace(operation, pageIndex, freeSpace);
          });
    }

    for (int i = 0; i < checks; i++) {
      final int freeSpace = random.nextInt(ODurablePage.MAX_PAGE_SIZE_BYTES);
      final int pageIndex = freeSpaceMap.findFreePage(freeSpace);
      final int freeSpaceIndex = freeSpace / FreeSpaceMap.NORMALIZATION_INTERVAL;
      if (freeSpaceIndex <= maxFreeSpaceIndex[0]) {
        Assert.assertTrue(pageSpaceMap.get(pageIndex) >= freeSpace);
      } else {
        Assert.assertEquals(-1, pageIndex);
      }
    }
  }

  @Test
  public void randomPagesUpdate() throws IOException {
    final int pages = 1_000;
    final int checks = 1_000;

    final HashMap<Integer, Integer> pageSpaceMap = new HashMap<>();
    final TreeMap<Integer, Integer> sizeMap = new TreeMap<>();

    final long seed = System.nanoTime();
    System.out.println("randomPagesUpdate seed - " + seed);

    final Random random = new Random(seed);

    for (int i = 0; i < pages; i++) {
      final int pageIndex = i;

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          operation -> {
            final int freeSpace = random.nextInt(ODurablePage.MAX_PAGE_SIZE_BYTES);
            pageSpaceMap.put(pageIndex, freeSpace);
            sizeMap.compute(
                freeSpace,
                (k, v) -> {
                  if (v == null) {
                    return 1;
                  }

                  return v + 1;
                });

            freeSpaceMap.updatePageFreeSpace(operation, pageIndex, freeSpace);
          });
    }

    for (int i = 0; i < pages; i++) {
      final int pageIndex = i;

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          operation -> {
            final int freeSpace = random.nextInt(ODurablePage.MAX_PAGE_SIZE_BYTES);
            final int oldFreeSpace = pageSpaceMap.get(pageIndex);

            pageSpaceMap.put(pageIndex, freeSpace);
            sizeMap.compute(
                freeSpace,
                (k, v) -> {
                  if (v == null) {
                    return 1;
                  }

                  return v + 1;
                });

            sizeMap.compute(
                oldFreeSpace,
                (k, v) -> {
                  //noinspection ConstantConditions
                  if (v == 1) {
                    return null;
                  }

                  return v - 1;
                });

            freeSpaceMap.updatePageFreeSpace(operation, pageIndex, freeSpace);
          });
    }

    final int maxFreeSpaceIndex =
        (sizeMap.lastKey() - (FreeSpaceMap.NORMALIZATION_INTERVAL - 1))
            / FreeSpaceMap.NORMALIZATION_INTERVAL;

    for (int i = 0; i < checks; i++) {
      final int freeSpace = random.nextInt(ODurablePage.MAX_PAGE_SIZE_BYTES);
      final int pageIndex = freeSpaceMap.findFreePage(freeSpace);
      final int freeSpaceIndex = freeSpace / FreeSpaceMap.NORMALIZATION_INTERVAL;

      if (freeSpaceIndex <= maxFreeSpaceIndex) {
        Assert.assertTrue(pageSpaceMap.get(pageIndex) >= freeSpace);
      } else {
        Assert.assertEquals(-1, pageIndex);
      }
    }
  }

  @After
  public void after() throws IOException {
    final OWriteCache writeCache = storage.getWriteCache();

    final long fileId = writeCache.fileIdByName(freeSpaceMap.getFullName());
    storage.getReadCache().deleteFile(fileId, writeCache);
  }
}
