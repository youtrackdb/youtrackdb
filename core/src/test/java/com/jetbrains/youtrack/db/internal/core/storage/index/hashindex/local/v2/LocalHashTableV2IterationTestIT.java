package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v2;

import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.BinarySerializerFactory;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.HashFunction;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.HashTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @since 13.03.13
 */
public class LocalHashTableV2IterationTestIT {

  private static final int KEYS_COUNT = 500000;

  private DatabaseSessionInternal db;

  private LocalHashTableV2<Integer, String> localHashTable;
  private AtomicOperationsManager atomicOperationsManager;

  @Before
  public void beforeClass() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null) {
      buildDirectory = ".";
    }

    db = new DatabaseDocumentTx("plocal:" + buildDirectory + "/localHashTableV2IterationTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    HashFunction<Integer> hashFunction = value -> Long.MAX_VALUE / 2 + value;

    atomicOperationsManager =
        ((AbstractPaginatedStorage) db.getStorage()).getAtomicOperationsManager();

    localHashTable =
        new LocalHashTableV2<Integer, String>(
            "localHashTableIterationTest",
            ".imc",
            ".tsc",
            ".obf",
            ".nbh",
            (AbstractPaginatedStorage) db.getStorage());

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            localHashTable.create(
                atomicOperation,
                IntegerSerializer.INSTANCE,
                BinarySerializerFactory.getInstance().getObjectSerializer(PropertyType.STRING),
                null,
                null,
                hashFunction,
                true));
  }

  @After
  public void afterClass() throws Exception {
    doClearTable();

    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> localHashTable.delete(atomicOperation));
    db.drop();
  }

  @After
  public void afterMethod() throws Exception {
    doClearTable();
  }

  private void doClearTable() throws IOException {
    final HashTable.Entry<Integer, String> firstEntry = localHashTable.firstEntry();

    if (firstEntry != null) {
      HashTable.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(firstEntry.key);
      while (entries.length > 0) {
        for (final HashTable.Entry<Integer, String> entry : entries) {
          atomicOperationsManager.executeInsideAtomicOperation(
              null, atomicOperation -> localHashTable.remove(atomicOperation, entry.key));
        }

        entries = localHashTable.higherEntries(entries[entries.length - 1].key);
      }
    }

    if (localHashTable.isNullKeyIsSupported()) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localHashTable.remove(atomicOperation, null));
    }
  }

  @Test
  public void testNextHaveRightOrder() throws Exception {
    SortedSet<Integer> keys = new TreeSet<>();
    final Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      if (localHashTable.get(key) == null) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
      }
    }

    HashTable.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(Integer.MIN_VALUE);
    int curPos = 0;
    for (int key : keys) {
      int sKey = entries[curPos].key;

      Assert.assertEquals(key, sKey);
      curPos++;
      if (curPos >= entries.length) {
        entries = localHashTable.higherEntries(entries[entries.length - 1].key);
        curPos = 0;
      }
    }
  }

  public void testNextSkipsRecordValid() throws Exception {
    List<Integer> keys = new ArrayList<>();

    final Random random = new Random();
    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      if (localHashTable.get(key) == null) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
      }
    }

    Collections.sort(keys);

    HashTable.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(keys.get(10));
    int curPos = 0;
    for (int key : keys) {
      if (key < keys.get(10)) {
        continue;
      }
      int sKey = entries[curPos].key;
      Assert.assertEquals(key, sKey);

      curPos++;
      if (curPos >= entries.length) {
        entries = localHashTable.higherEntries(entries[entries.length - 1].key);
        curPos = 0;
      }
    }
  }

  @Test
  @Ignore
  public void testNextHaveRightOrderUsingNextMethod() throws Exception {
    List<Integer> keys = new ArrayList<>();
    Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt();

      if (localHashTable.get(key) == null) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> localHashTable.put(atomicOperation, key, String.valueOf(key)));
        keys.add(key);
        Assert.assertEquals(localHashTable.get(key), String.valueOf(key));
      }
    }

    Collections.sort(keys);

    for (int key : keys) {
      HashTable.Entry<Integer, String>[] entries = localHashTable.ceilingEntries(key);
      Assert.assertEquals(key, (int) entries[0].key);
    }

    for (int j = 0, keysSize = keys.size() - 1; j < keysSize; j++) {
      int key = keys.get(j);
      int sKey = localHashTable.higherEntries(key)[0].key;
      Assert.assertEquals(sKey, (int) keys.get(j + 1));
    }
  }
}
