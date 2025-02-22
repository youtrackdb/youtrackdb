package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.singlevalue.v1;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.HighLevelException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.serialization.types.UTF8Serializer;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CellBTreeSingleValueV1TestIT {

  private AtomicOperationsManager atomicOperationsManager;
  private CellBTreeSingleValueV1<String> singleValueTree;
  private YouTrackDB youTrackDB;

  private String dbName;

  @Before
  public void before() throws Exception {
    final String buildDirectory =
        System.getProperty("buildDirectory", ".")
            + File.separator
            + CellBTreeSingleValueV1TestIT.class.getSimpleName();

    dbName = "localSingleBTreeTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    FileUtils.deleteRecursively(dbDirectory);

    YouTrackDBConfig config = YouTrackDBConfig.builder().build();
    youTrackDB = new YouTrackDBImpl("plocal:" + buildDirectory, config);
    youTrackDB.execute(
        "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");

    AbstractPaginatedStorage storage;
    try (DatabaseSession databaseDocumentTx = youTrackDB.open(dbName, "admin", "admin")) {
      storage =
          (AbstractPaginatedStorage) ((DatabaseSessionInternal) databaseDocumentTx).getStorage();
    }

    atomicOperationsManager = storage.getAtomicOperationsManager();
    singleValueTree = new CellBTreeSingleValueV1<>("singleBTree", ".sbt", ".nbt", storage);
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            singleValueTree.create(atomicOperation, UTF8Serializer.INSTANCE, null, 1));
  }

  @After
  public void afterMethod() {
    youTrackDB.drop(dbName);
    youTrackDB.close();
  }

  @Test
  public void testKeyPut() throws Exception {
    final int keysCount = 1_000_000;

    final int rollbackInterval = 100;
    final String[] lastKey = new String[1];
    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int iterationCounter = i;
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final String key = Integer.toString(iterationCounter * rollbackInterval + j);
                  singleValueTree.put(
                      atomicOperation,
                      key,
                      new RecordId(
                          (iterationCounter * rollbackInterval + j) % 32000,
                          iterationCounter * rollbackInterval + j));

                  if (rollbackCounter == 1) {
                    if ((iterationCounter * rollbackInterval + j) % 100_000 == 0) {
                      System.out.printf(
                          "%d items loaded out of %d%n",
                          iterationCounter * rollbackInterval + j, keysCount);
                    }

                    if (lastKey[0] == null) {
                      lastKey[0] = key;
                    } else if (key.compareTo(lastKey[0]) > 0) {
                      lastKey[0] = key;
                    }
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }

      Assert.assertEquals("0", singleValueTree.firstKey());
      Assert.assertEquals(lastKey[0], singleValueTree.lastKey());
    }

    for (int i = 0; i < keysCount; i++) {
      Assert.assertEquals(
          i + " key is absent",
          new RecordId(i % 32000, i),
          singleValueTree.get(Integer.toString(i)));
      if (i % 100_000 == 0) {
        System.out.printf("%d items tested out of %d%n", i, keysCount);
      }
    }

    for (int i = keysCount; i < 2 * keysCount; i++) {
      Assert.assertNull(singleValueTree.get(Integer.toString(i)));
    }
  }

  @Test
  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<String> keys = new TreeSet<>();
    final Random random = new Random();
    final int keysCount = 1_000_000;

    final int rollbackRange = 100;
    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;

        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int i = 0; i < rollbackRange; i++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);

                  singleValueTree.put(atomicOperation, key, new RecordId(val % 32000, val));

                  if (rollbackCounter == 1) {
                    keys.add(key);
                  }
                  Assert.assertEquals(singleValueTree.get(key), new RecordId(val % 32000, val));
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }

    Assert.assertEquals(singleValueTree.firstKey(), keys.first());
    Assert.assertEquals(singleValueTree.lastKey(), keys.last());

    for (String key : keys) {
      final int val = Integer.parseInt(key);
      Assert.assertEquals(singleValueTree.get(key), new RecordId(val % 32000, val));
    }
  }

  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    NavigableSet<String> keys = new TreeSet<>();
    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);
    final int keysCount = 1_000_000;
    final int rollbackRange = 100;

    while (keys.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;

        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int i = 0; i < rollbackRange; i++) {
                  int val;
                  do {
                    val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
                  } while (val < 0);

                  String key = Integer.toString(val);
                  singleValueTree.put(atomicOperation, key, new RecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keys.add(key);
                  }

                  Assert.assertEquals(singleValueTree.get(key), new RecordId(val % 32000, val));
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }

    Assert.assertEquals(singleValueTree.firstKey(), keys.first());
    Assert.assertEquals(singleValueTree.lastKey(), keys.last());

    for (String key : keys) {
      int val = Integer.parseInt(key);
      Assert.assertEquals(singleValueTree.get(key), new RecordId(val % 32000, val));
    }
  }

  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    final int keysCount = 1_000_000;

    NavigableSet<String> keys = new TreeSet<>();
    for (int i = 0; i < keysCount; i++) {
      String key = Integer.toString(i);
      final int k = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              singleValueTree.put(atomicOperation, key, new RecordId(k % 32000, k)));
      keys.add(key);
    }

    final int rollbackInterval = 10;
    Iterator<String> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      String key = keysIterator.next();

      if (Integer.parseInt(key) % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> singleValueTree.remove(atomicOperation, key));
        keysIterator.remove();
      }

      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              int rollbackCounter = 0;
              final Iterator<String> keysDeletionIterator = keys.tailSet(key, false).iterator();
              while (keysDeletionIterator.hasNext() && rollbackCounter < rollbackInterval) {
                String keyToDelete = keysDeletionIterator.next();
                rollbackCounter++;
                singleValueTree.remove(atomicOperation, keyToDelete);
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
    }

    Assert.assertEquals(singleValueTree.firstKey(), keys.first());
    Assert.assertEquals(singleValueTree.lastKey(), keys.last());

    for (String key : keys) {
      int val = Integer.parseInt(key);
      if (val % 3 == 0) {
        Assert.assertNull(singleValueTree.get(key));
      } else {
        Assert.assertEquals(singleValueTree.get(key), new RecordId(val % 32000, val));
      }
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws Exception {
    NavigableSet<String> keys = new TreeSet<>();
    final int keysCount = 1_000_000;

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < keysCount) {
      int val = (int) (random.nextGaussian() * Integer.MAX_VALUE / 2 + Integer.MAX_VALUE);
      if (val < 0) {
        continue;
      }

      String key = Integer.toString(val);
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              singleValueTree.put(atomicOperation, key, new RecordId(val % 32000, val)));
      keys.add(key);

      Assert.assertEquals(singleValueTree.get(key), new RecordId(val % 32000, val));
    }

    Iterator<String> keysIterator = keys.iterator();

    final int rollbackInterval = 10;
    while (keysIterator.hasNext()) {
      String key = keysIterator.next();

      if (Integer.parseInt(key) % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> singleValueTree.remove(atomicOperation, key));
        keysIterator.remove();
      }

      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation -> {
              int rollbackCounter = 0;
              final Iterator<String> keysDeletionIterator = keys.tailSet(key, false).iterator();
              while (keysDeletionIterator.hasNext() && rollbackCounter < rollbackInterval) {
                String keyToDelete = keysDeletionIterator.next();
                rollbackCounter++;
                singleValueTree.remove(atomicOperation, keyToDelete);
              }
              throw new RollbackException();
            });
      } catch (RollbackException ignore) {
      }
    }

    Assert.assertEquals(singleValueTree.firstKey(), keys.first());
    Assert.assertEquals(singleValueTree.lastKey(), keys.last());

    for (String key : keys) {
      int val = Integer.parseInt(key);
      if (val % 3 == 0) {
        Assert.assertNull(singleValueTree.get(key));
      } else {
        Assert.assertEquals(singleValueTree.get(key), new RecordId(val % 32000, val));
      }
    }
  }

  @Test
  public void testKeyDelete() throws Exception {
    final int keysCount = 1_000_000;

    for (int i = 0; i < keysCount; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              singleValueTree.put(
                  atomicOperation, Integer.toString(key), new RecordId(key % 32000, key)));
    }

    final int rollbackInterval = 100;

    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int iterationCounter = i;
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final int key = iterationCounter * rollbackInterval + j;
                  if (key % 3 == 0) {
                    Assert.assertEquals(
                        singleValueTree.remove(atomicOperation, Integer.toString(key)),
                        new RecordId(key % 32000, key));
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(singleValueTree.get(Integer.toString(i)));
      } else {
        Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new RecordId(i % 32000, i));
      }
    }
  }

  @Test
  public void testKeyAddDelete() throws Exception {
    final int keysCount = 1_000_000;

    for (int i = 0; i < keysCount; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation ->
              singleValueTree.put(
                  atomicOperation, Integer.toString(key), new RecordId(key % 32000, key)));

      Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new RecordId(i % 32000, i));
    }

    final int rollbackInterval = 100;

    for (int i = 0; i < keysCount / rollbackInterval; i++) {
      for (int n = 0; n < 2; n++) {
        final int iterationCounter = i;
        final int rollbackCounter = n;

        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  final int key = iterationCounter * rollbackInterval + j;

                  if (key % 3 == 0) {
                    Assert.assertEquals(
                        singleValueTree.remove(atomicOperation, Integer.toString(key)),
                        new RecordId(key % 32000, key));
                  }

                  if (key % 2 == 0) {
                    singleValueTree.put(
                        atomicOperation,
                        Integer.toString(keysCount + key),
                        new RecordId((keysCount + key) % 32000, keysCount + key));
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(singleValueTree.get(Integer.toString(i)));
      } else {
        Assert.assertEquals(singleValueTree.get(Integer.toString(i)), new RecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        Assert.assertEquals(
            singleValueTree.get(Integer.toString(keysCount + i)),
            new RecordId((keysCount + i) % 32000, keysCount + i));
      }
    }
  }

  @Test
  public void testKeyCursor() throws Exception {
    final int keysCount = 1_000_000;

    NavigableMap<String, RID> keyValues = new TreeMap<>();
    final long seed = System.nanoTime();

    System.out.println("testKeyCursor: " + seed);
    Random random = new Random(seed);

    final int rollbackInterval = 100;

    int printCounter = 0;
    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);

                  singleValueTree.put(atomicOperation, key, new RecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(key, new RecordId(val % 32000, val));
                  }
                }

                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }

      if (keyValues.size() > printCounter * 100_000) {
        System.out.println(keyValues.size() + " entries were added.");
        printCounter++;
      }
    }

    Assert.assertEquals(singleValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(singleValueTree.lastKey(), keyValues.lastKey());

    final Iterator<String> indexIterator;
    try (Stream<String> stream = singleValueTree.keyStream()) {
      indexIterator = stream.iterator();
      for (String entryKey : keyValues.keySet()) {
        final String indexKey = indexIterator.next();
        Assert.assertEquals(entryKey, indexKey);
      }
    }
  }

  @Test
  public void testIterateEntriesMajor() throws Exception {
    final int keysCount = 1_000_000;

    NavigableMap<String, RID> keyValues = new TreeMap<>();
    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMajor: " + seed);
    final Random random = new Random(seed);

    final int rollbackInterval = 100;
    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;

        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);

                  singleValueTree.put(atomicOperation, key, new RecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(key, new RecordId(val % 32000, val));
                  }
                }

                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }

      if (keyValues.size() > printCounter * 100_000) {
        System.out.println(keyValues.size() + " entries were added.");
        printCounter++;
      }
    }

    assertIterateMajorEntries(keyValues, random, true, true);
    assertIterateMajorEntries(keyValues, random, false, true);

    assertIterateMajorEntries(keyValues, random, true, false);
    assertIterateMajorEntries(keyValues, random, false, false);

    Assert.assertEquals(singleValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(singleValueTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesMinor() throws Exception {
    final int keysCount = 1_000_000;
    NavigableMap<String, RID> keyValues = new TreeMap<>();

    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMinor: " + seed);
    final Random random = new Random(seed);

    final int rollbackInterval = 100;

    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);

                  singleValueTree.put(atomicOperation, key, new RecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(key, new RecordId(val % 32000, val));
                  }
                }
                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }
      }

      if (keyValues.size() > printCounter * 100_000) {
        System.out.println(keyValues.size() + " entries were added.");
        printCounter++;
      }
    }

    assertIterateMinorEntries(keyValues, random, true, true);
    assertIterateMinorEntries(keyValues, random, false, true);

    assertIterateMinorEntries(keyValues, random, true, false);
    assertIterateMinorEntries(keyValues, random, false, false);

    Assert.assertEquals(singleValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(singleValueTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesBetween() throws Exception {
    final int keysCount = 1_000_000;
    NavigableMap<String, RID> keyValues = new TreeMap<>();
    final Random random = new Random();

    final int rollbackInterval = 100;
    int printCounter = 0;

    while (keyValues.size() < keysCount) {
      for (int n = 0; n < 2; n++) {
        final int rollbackCounter = n;
        try {
          atomicOperationsManager.executeInsideAtomicOperation(
              null,
              atomicOperation -> {
                for (int j = 0; j < rollbackInterval; j++) {
                  int val = random.nextInt(Integer.MAX_VALUE);
                  String key = Integer.toString(val);

                  singleValueTree.put(atomicOperation, key, new RecordId(val % 32000, val));
                  if (rollbackCounter == 1) {
                    keyValues.put(key, new RecordId(val % 32000, val));
                  }
                }

                if (rollbackCounter == 0) {
                  throw new RollbackException();
                }
              });
        } catch (RollbackException ignore) {
        }

        if (keyValues.size() > printCounter * 100_000) {
          System.out.println(keyValues.size() + " entries were added.");
          printCounter++;
        }
      }
    }

    assertIterateBetweenEntries(keyValues, random, true, true, true);
    assertIterateBetweenEntries(keyValues, random, true, false, true);
    assertIterateBetweenEntries(keyValues, random, false, true, true);
    assertIterateBetweenEntries(keyValues, random, false, false, true);

    assertIterateBetweenEntries(keyValues, random, true, true, false);
    assertIterateBetweenEntries(keyValues, random, true, false, false);
    assertIterateBetweenEntries(keyValues, random, false, true, false);
    assertIterateBetweenEntries(keyValues, random, false, false, false);

    Assert.assertEquals(singleValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(singleValueTree.lastKey(), keyValues.lastKey());
  }

  private void assertIterateMajorEntries(
      NavigableMap<String, RID> keyValues,
      Random random,
      boolean keyInclusive,
      boolean ascSortOrder) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      final int fromKeyIndex = random.nextInt(keys.length);
      String fromKey = keys[fromKeyIndex];

      if (random.nextBoolean()) {
        fromKey =
            fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      final Iterator<RawPair<String, RID>> indexIterator;
      try (Stream<RawPair<String, RID>> stream =
          singleValueTree.iterateEntriesMajor(fromKey, keyInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();
        Iterator<Map.Entry<String, RID>> iterator;
        if (ascSortOrder) {
          iterator = keyValues.tailMap(fromKey, keyInclusive).entrySet().iterator();
        } else {
          iterator =
              keyValues
                  .descendingMap()
                  .subMap(keyValues.lastKey(), true, fromKey, keyInclusive)
                  .entrySet()
                  .iterator();
        }

        while (iterator.hasNext()) {
          final RawPair<String, RID> indexEntry = indexIterator.next();
          final Map.Entry<String, RID> entry = iterator.next();

          Assert.assertEquals(indexEntry.first, entry.getKey());
          Assert.assertEquals(indexEntry.second, entry.getValue());
        }

        //noinspection ConstantConditions
        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(indexIterator.hasNext());
      }
    }
  }

  private void assertIterateMinorEntries(
      NavigableMap<String, RID> keyValues,
      Random random,
      boolean keyInclusive,
      boolean ascSortOrder) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      int toKeyIndex = random.nextInt(keys.length);
      String toKey = keys[toKeyIndex];
      if (random.nextBoolean()) {
        toKey = toKey.substring(0, toKey.length() - 2) + (toKey.charAt(toKey.length() - 1) + 1);
      }

      final Iterator<RawPair<String, RID>> indexIterator;
      try (Stream<RawPair<String, RID>> stream =
          singleValueTree.iterateEntriesMinor(toKey, keyInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();
        Iterator<Map.Entry<String, RID>> iterator;
        if (ascSortOrder) {
          iterator = keyValues.headMap(toKey, keyInclusive).entrySet().iterator();
        } else {
          iterator = keyValues.headMap(toKey, keyInclusive).descendingMap().entrySet().iterator();
        }

        while (iterator.hasNext()) {
          RawPair<String, RID> indexEntry = indexIterator.next();
          Map.Entry<String, RID> entry = iterator.next();

          Assert.assertEquals(indexEntry.first, entry.getKey());
          Assert.assertEquals(indexEntry.second, entry.getValue());
        }

        //noinspection ConstantConditions
        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(indexIterator.hasNext());
      }
    }
  }

  private void assertIterateBetweenEntries(
      NavigableMap<String, RID> keyValues,
      Random random,
      boolean fromInclusive,
      boolean toInclusive,
      boolean ascSortOrder) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      int fromKeyIndex = random.nextInt(keys.length);
      int toKeyIndex = random.nextInt(keys.length);

      if (fromKeyIndex > toKeyIndex) {
        toKeyIndex = fromKeyIndex;
      }

      String fromKey = keys[fromKeyIndex];
      String toKey = keys[toKeyIndex];

      if (random.nextBoolean()) {
        fromKey =
            fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      if (random.nextBoolean()) {
        toKey = toKey.substring(0, toKey.length() - 2) + (toKey.charAt(toKey.length() - 1) + 1);
      }

      if (fromKey.compareTo(toKey) > 0) {
        fromKey = toKey;
      }

      final Iterator<RawPair<String, RID>> indexIterator;
      try (Stream<RawPair<String, RID>> stream =
          singleValueTree.iterateEntriesBetween(
              fromKey, fromInclusive, toKey, toInclusive, ascSortOrder)) {
        indexIterator = stream.iterator();

        Iterator<Map.Entry<String, RID>> iterator;
        if (ascSortOrder) {
          iterator =
              keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).entrySet().iterator();
        } else {
          iterator =
              keyValues
                  .descendingMap()
                  .subMap(toKey, toInclusive, fromKey, fromInclusive)
                  .entrySet()
                  .iterator();
        }

        while (iterator.hasNext()) {
          RawPair<String, RID> indexEntry = indexIterator.next();
          Assert.assertNotNull(indexEntry);

          Map.Entry<String, RID> mapEntry = iterator.next();
          Assert.assertEquals(indexEntry.first, mapEntry.getKey());
          Assert.assertEquals(indexEntry.second, mapEntry.getValue());
        }
        //noinspection ConstantConditions
        Assert.assertFalse(iterator.hasNext());
        Assert.assertFalse(indexIterator.hasNext());
      }
    }
  }

  static final class RollbackException extends BaseException implements HighLevelException {

    public RollbackException() {
      this("");
    }

    public RollbackException(String message) {
      super(message);
    }

    @SuppressWarnings("unused")
    public RollbackException(RollbackException exception) {
      super(exception);
    }
  }
}
