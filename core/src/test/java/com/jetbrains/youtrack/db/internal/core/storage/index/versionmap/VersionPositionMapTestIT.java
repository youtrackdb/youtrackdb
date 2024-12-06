package com.jetbrains.youtrack.db.internal.core.storage.index.versionmap;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import java.io.File;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class VersionPositionMapTestIT {

  public static final String DIR_NAME = "/" + VersionPositionMapTestIT.class.getSimpleName();
  public static final String DB_NAME = "versionPositionMapTest";
  private static YouTrackDB youTrackDB;
  private static AtomicOperationsManager atomicOperationsManager;
  private static AbstractPaginatedStorage storage;
  private static String buildDirectory;

  private VersionPositionMapV0 versionPositionMap;

  @BeforeClass
  public static void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null) {
      buildDirectory = "./target/databases" + DIR_NAME;
    } else {
      buildDirectory += DIR_NAME;
    }
    FileUtils.deleteRecursively(new File(buildDirectory));

    youTrackDB =
        CreateDatabaseUtil.createDatabase(
            DB_NAME, DbTestBase.embeddedDBUrl(VersionPositionMapTestIT.class),
            CreateDatabaseUtil.TYPE_PLOCAL);
    if (youTrackDB.exists(DB_NAME)) {
      youTrackDB.drop(DB_NAME);
    }
    CreateDatabaseUtil.createDatabase(DB_NAME, youTrackDB, CreateDatabaseUtil.TYPE_PLOCAL);

    DatabaseSession databaseSession =
        youTrackDB.open(DB_NAME, "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    storage = (AbstractPaginatedStorage) ((DatabaseSessionInternal) databaseSession).getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();
    databaseSession.close();
  }

  @AfterClass
  public static void afterClass() {
    youTrackDB.drop(DB_NAME);
    youTrackDB.close();
    FileUtils.deleteRecursively(new File(buildDirectory));
  }

  @Before
  public void setUp() throws Exception {
    final String name = "Person.name";
    versionPositionMap =
        new VersionPositionMapV0(storage, name, name + ".cbt", VersionPositionMap.DEF_EXTENSION);
    final AtomicOperation atomicOperation = atomicOperationsManager.startAtomicOperation(null);
    versionPositionMap.create(atomicOperation);
    Assert.assertEquals("Number of pages do not match", 1, versionPositionMap.getNumberOfPages());
    versionPositionMap.open();
  }

  @After
  public void tearDown() throws Exception {
    final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    versionPositionMap.delete(atomicOperation);
    atomicOperationsManager.alarmClearOfAtomicOperation();
  }

  @Test
  public void testIncrementVersion() throws Exception {
    final int maxVPMSize = VersionPositionMap.DEFAULT_VERSION_ARRAY_SIZE;
    for (int hash = 0; hash <= maxVPMSize; hash++) {
      final int version = versionPositionMap.getVersion(hash);
      versionPositionMap.updateVersion(hash);
      Assert.assertEquals(version + 1, versionPositionMap.getVersion(hash));
    }
  }

  @Test
  public void testMultiIncrementVersion() throws Exception {
    final int maxVPMSize = VersionPositionMap.DEFAULT_VERSION_ARRAY_SIZE;
    final int maxVersionNumber = Integer.SIZE;
    for (int hash = 0; hash <= maxVPMSize; hash++) {
      for (int j = 0; j < maxVersionNumber; j++) {
        versionPositionMap.updateVersion(hash);
      }
      Assert.assertEquals(maxVersionNumber, versionPositionMap.getVersion(hash));
    }
  }

  @Test
  public void testRandomIncrementVersion() throws Exception {
    final int maxVPMSize = VersionPositionMap.DEFAULT_VERSION_ARRAY_SIZE;
    final long seed = System.nanoTime();
    System.out.printf("incrementVersion seed :%d%n", seed);
    final Random random = new Random(seed);
    for (int i = 0; i <= maxVPMSize; i++) {
      int randomNum = random.nextInt((maxVPMSize) + 1);
      final int version = versionPositionMap.getVersion(randomNum);
      versionPositionMap.updateVersion(randomNum);
      Assert.assertEquals(version + 1, versionPositionMap.getVersion(randomNum));
    }
  }

  @Test
  public void testGetKeyHash() {
    Assert.assertEquals(0, versionPositionMap.getKeyHash(null));
    Assert.assertEquals(4659, versionPositionMap.getKeyHash("YouTrackDB"));
    Assert.assertEquals(
        3988,
        versionPositionMap.getKeyHash("07d_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"));
    Assert.assertEquals(
        0, versionPositionMap.getKeyHash(VersionPositionMap.DEFAULT_VERSION_ARRAY_SIZE));
    Assert.assertEquals(0, versionPositionMap.getKeyHash(0));
  }

  @Test
  public void testGracefulOldStorageHandling() throws Exception {
    final AtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
    versionPositionMap.delete(atomicOperation);
    versionPositionMap.open();
    versionPositionMap.getVersion(0);
  }
}
