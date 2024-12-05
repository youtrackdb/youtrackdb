package com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local;

import com.jetbrains.youtrack.db.internal.common.serialization.types.OIntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.YTDatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OAbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @since 12.08.13
 */
public class OSBTreeBonsaiLocalTestIT {

  private static final int KEYS_COUNT = 500000;
  protected static OSBTreeBonsaiLocal<Integer, YTIdentifiable> sbTree;
  protected static YTDatabaseSessionInternal db;
  private static OAtomicOperationsManager atomicOperationsManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null) {
      buildDirectory = "./target";
    }

    db = new YTDatabaseDocumentTx("plocal:" + buildDirectory + "/localSBTreeBonsaiTest");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    sbTree =
        new OSBTreeBonsaiLocal<>(
            "actualSBTreeBonsaiLocalTest", ".irs", (OAbstractPaginatedStorage) db.getStorage());

    atomicOperationsManager =
        ((OAbstractPaginatedStorage) db.getStorage()).getAtomicOperationsManager();
    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> sbTree.createComponent(atomicOperation));

    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            sbTree.create(atomicOperation, OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation -> {
          sbTree.clear(atomicOperation);
          sbTree.delete(atomicOperation);
          sbTree.deleteComponent(atomicOperation);
        });

    db.drop();
  }

  @After
  public void afterMethod() throws Exception {
    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> sbTree.clear(atomicOperation));
  }

  @Test
  public void testGetFisrtKeyInEmptyTree() {
    Integer result = sbTree.firstKey();

    Assert.assertNull(result);
  }

  @Test
  public void testKeyPut() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      //      Assert.assertEquals(sbTree.get(i), new YTRecordId(i % 32000, i), i + " key is absent");
      Assertions.assertThat(sbTree.get(i)).isEqualTo(new YTRecordId(i % 32000, i));
    }

    Assert.assertEquals(0, (int) sbTree.firstKey());
    Assert.assertEquals(KEYS_COUNT - 1, (int) sbTree.lastKey());

    for (int i = KEYS_COUNT; i < 2 * KEYS_COUNT; i++) {
      Assert.assertNull(sbTree.get(i));
    }
  }

  @Test
  public void testKeyPutRandomUniform() throws Exception {
    final NavigableSet<Integer> keys = new TreeSet<>();
    final Random random = new Random();

    while (keys.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new YTRecordId(key % 32000, key));
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys) {
      Assert.assertEquals(sbTree.get(key), new YTRecordId(key % 32000, key));
    }
  }

  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    final double mx = Integer.MAX_VALUE / 2.;
    final double dx = Integer.MAX_VALUE / 8.;

    NavigableSet<Integer> keys = new TreeSet<>();
    long seed = System.currentTimeMillis();

    System.out.println("testKeyPutRandomGaussian seed : " + seed);

    Random random = new Random(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = generateGaussianKey(mx, dx, random);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new YTRecordId(key % 32000, key));
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys) {
      Assert.assertEquals(sbTree.get(key), new YTRecordId(key % 32000, key));
    }
  }

  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    NavigableSet<Integer> keys = new TreeSet<>();
    for (int i = 0; i < KEYS_COUNT; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
      keys.add(i);
    }

    Iterator<Integer> keysIterator = keys.iterator();
    while (keysIterator.hasNext()) {
      int key = keysIterator.next();
      if (key % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sbTree.remove(atomicOperation, key));
        keysIterator.remove();
      }
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(sbTree.get(key));
      } else {
        Assert.assertEquals(sbTree.get(key), new YTRecordId(key % 32000, key));
      }
    }
  }

  @Test
  public void testKeyDeleteRandomGaussian() throws Exception {
    final double mx = Integer.MAX_VALUE / 2.;
    final double dx = Integer.MAX_VALUE / 8.;

    NavigableSet<Integer> keys = new TreeSet<>();

    long seed = System.currentTimeMillis();

    System.out.println("testKeyDeleteRandomGaussian seed : " + seed);
    Random random = new Random(seed);

    while (keys.size() < KEYS_COUNT) {
      int key = generateGaussianKey(mx, dx, random);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
      keys.add(key);

      Assert.assertEquals(sbTree.get(key), new YTRecordId(key % 32000, key));
    }

    Iterator<Integer> keysIterator = keys.iterator();

    while (keysIterator.hasNext()) {
      int key = keysIterator.next();

      if (key % 3 == 0) {
        atomicOperationsManager.executeInsideAtomicOperation(
            null, atomicOperation -> sbTree.remove(atomicOperation, key));
        keysIterator.remove();
      }
    }

    Assert.assertEquals(sbTree.firstKey(), keys.first());
    Assert.assertEquals(sbTree.lastKey(), keys.last());

    for (int key : keys) {
      if (key % 3 == 0) {
        Assert.assertNull(sbTree.get(key));
      } else {
        Assert.assertEquals(sbTree.get(key), new YTRecordId(key % 32000, key));
      }
    }
  }

  @Test
  public void testKeyDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertEquals(
                    sbTree.remove(atomicOperation, key), new YTRecordId(key % 32000, key)));
      }
    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);
    Assert.assertEquals(
        (int) sbTree.lastKey(), (KEYS_COUNT - 1) % 3 == 0 ? KEYS_COUNT - 2 : KEYS_COUNT - 1);

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(sbTree.get(i));
      } else {
        Assert.assertEquals(sbTree.get(i), new YTRecordId(i % 32000, i));
      }
    }
  }

  @Test
  public void testKeyAddDelete() throws Exception {
    for (int i = 0; i < KEYS_COUNT; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));

      Assert.assertEquals(sbTree.get(i), new YTRecordId(i % 32000, i));
    }

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                Assert.assertEquals(
                    sbTree.remove(atomicOperation, key), new YTRecordId(key % 32000, key)));
      }

      if (i % 2 == 0) {
        final int key = i;
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                sbTree.put(
                    atomicOperation,
                    KEYS_COUNT + key,
                    new YTRecordId((KEYS_COUNT + key) % 32000, KEYS_COUNT + key)));
      }
    }

    Assert.assertEquals((int) sbTree.firstKey(), 1);
    Assert.assertEquals((int) sbTree.lastKey(), 2 * KEYS_COUNT - 2);

    for (int i = 0; i < KEYS_COUNT; i++) {
      if (i % 3 == 0) {
        Assert.assertNull(sbTree.get(i));
      } else {
        Assert.assertEquals(sbTree.get(i), new YTRecordId(i % 32000, i));
      }

      if (i % 2 == 0) {
        Assert.assertEquals(
            sbTree.get(KEYS_COUNT + i), new YTRecordId((KEYS_COUNT + i) % 32000, KEYS_COUNT + i));
      }
    }
  }

  @Test
  public void testValuesMajor() throws Exception {
    NavigableMap<Integer, YTRID> keyValues = new TreeMap<>();
    Random random = new Random();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
      keyValues.put(key, new YTRecordId(key % 32000, key));
    }

    assertMajorValues(keyValues, random, true);
    assertMajorValues(keyValues, random, false);

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testValuesMinor() throws Exception {
    NavigableMap<Integer, YTRID> keyValues = new TreeMap<>();
    Random random = new Random();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
      keyValues.put(key, new YTRecordId(key % 32000, key));
    }

    assertMinorValues(keyValues, random, true);
    assertMinorValues(keyValues, random, false);

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testValuesBetween() throws Exception {
    NavigableMap<Integer, YTRID> keyValues = new TreeMap<>();
    Random random = new Random();

    while (keyValues.size() < KEYS_COUNT) {
      int key = random.nextInt(Integer.MAX_VALUE);

      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
      keyValues.put(key, new YTRecordId(key % 32000, key));
    }

    assertBetweenValues(keyValues, random, true, true);
    assertBetweenValues(keyValues, random, true, false);
    assertBetweenValues(keyValues, random, false, true);
    assertBetweenValues(keyValues, random, false, false);

    Assert.assertEquals(sbTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(sbTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
    for (int i = 0; i < 110; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
    }

    for (int i = 0; i < 56; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    Assert.assertEquals((int) sbTree.firstKey(), 56);

    for (int i = 0; i < 56; i++) {
      Assert.assertNull(sbTree.get(i));
    }

    for (int i = 56; i < 110; i++) {
      Assert.assertEquals(sbTree.get(i), new YTRecordId(i % 32000, i));
    }
  }

  @Test
  public void testAddKeyValuesInTwoBucketsAndMakeLastEmpty() throws Exception {
    for (int i = 0; i < 110; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
    }

    for (int i = 110; i > 50; i--) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    Assert.assertEquals((int) sbTree.lastKey(), 50);

    for (int i = 110; i > 50; i--) {
      Assert.assertNull(sbTree.get(i));
    }

    for (int i = 50; i >= 0; i--) {
      Assert.assertEquals(sbTree.get(i), new YTRecordId(i % 32000, i));
    }
  }

  @Test
  public void testAddKeyValuesAndRemoveFirstMiddleAndLastPages() throws Exception {
    for (int i = 0; i < 326; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null,
          atomicOperation -> sbTree.put(atomicOperation, key, new YTRecordId(key % 32000, key)));
    }

    for (int i = 0; i < 60; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    for (int i = 100; i < 220; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    for (int i = 260; i < 326; i++) {
      final int key = i;
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> sbTree.remove(atomicOperation, key));
    }

    Assert.assertEquals((int) sbTree.firstKey(), 60);
    Assert.assertEquals((int) sbTree.lastKey(), 259);

    Collection<YTIdentifiable> result = sbTree.getValuesMinor(250, true, -1);

    Set<YTIdentifiable> identifiables = new HashSet<>(result);
    for (int i = 250; i >= 220; i--) {
      boolean removed = identifiables.remove(new YTRecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 99; i >= 60; i--) {
      boolean removed = identifiables.remove(new YTRecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    result = sbTree.getValuesMajor(70, true, -1);
    identifiables = new HashSet<>(result);

    for (int i = 70; i < 100; i++) {
      boolean removed = identifiables.remove(new YTRecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 220; i < 260; i++) {
      boolean removed = identifiables.remove(new YTRecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());

    result = sbTree.getValuesBetween(70, true, 250, true, -1);
    identifiables = new HashSet<>(result);

    for (int i = 70; i < 100; i++) {
      boolean removed = identifiables.remove(new YTRecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    for (int i = 220; i <= 250; i++) {
      boolean removed = identifiables.remove(new YTRecordId(i % 32000, i));
      Assert.assertTrue(removed);
    }

    Assert.assertTrue(identifiables.isEmpty());
  }

  private static int generateGaussianKey(double mx, double dx, Random random) {
    double v;
    do {
      v = random.nextGaussian() * dx + mx;
    } while (v < 0 || v > Integer.MAX_VALUE);
    return (int) v;
  }

  private static void assertMajorValues(
      NavigableMap<Integer, YTRID> keyValues, Random random, boolean keyInclusive) {
    for (int i = 0; i < 100; i++) {
      int upperBorder = keyValues.lastKey() + 5000;
      int fromKey;
      if (upperBorder > 0) {
        fromKey = random.nextInt(upperBorder);
      } else {
        fromKey = random.nextInt(Integer.MAX_VALUE);
      }

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(fromKey);
        if (includedKey != null) {
          fromKey = includedKey;
        } else {
          fromKey = keyValues.floorKey(fromKey);
        }
      }

      int maxValuesToFetch = 10000;
      Collection<YTIdentifiable> orids =
          sbTree.getValuesMajor(fromKey, keyInclusive, maxValuesToFetch);

      Set<YTIdentifiable> result = new HashSet<>(orids);

      Iterator<YTRID> valuesIterator = keyValues.tailMap(fromKey, keyInclusive).values().iterator();

      int fetchedValues = 0;
      while (valuesIterator.hasNext() && fetchedValues < maxValuesToFetch) {
        YTRID value = valuesIterator.next();
        Assert.assertTrue(result.remove(value));

        fetchedValues++;
      }

      if (valuesIterator.hasNext()) {
        Assert.assertEquals(fetchedValues, maxValuesToFetch);
      }

      Assert.assertEquals(result.size(), 0);
    }
  }

  private static void assertMinorValues(
      NavigableMap<Integer, YTRID> keyValues, Random random, boolean keyInclusive) {
    for (int i = 0; i < 100; i++) {
      int upperBorder = keyValues.lastKey() + 5000;
      int toKey;
      if (upperBorder > 0) {
        toKey = random.nextInt(upperBorder) - 5000;
      } else {
        toKey = random.nextInt(Integer.MAX_VALUE) - 5000;
      }

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(toKey);
        if (includedKey != null) {
          toKey = includedKey;
        } else {
          toKey = keyValues.floorKey(toKey);
        }
      }

      int maxValuesToFetch = 10000;
      Collection<YTIdentifiable> orids =
          sbTree.getValuesMinor(toKey, keyInclusive, maxValuesToFetch);

      Set<YTIdentifiable> result = new HashSet<>(orids);

      Iterator<YTRID> valuesIterator =
          keyValues.headMap(toKey, keyInclusive).descendingMap().values().iterator();

      int fetchedValues = 0;
      while (valuesIterator.hasNext() && fetchedValues < maxValuesToFetch) {
        YTRID value = valuesIterator.next();
        Assert.assertTrue(result.remove(value));

        fetchedValues++;
      }

      if (valuesIterator.hasNext()) {
        Assert.assertEquals(fetchedValues, maxValuesToFetch);
      }

      Assert.assertEquals(result.size(), 0);
    }
  }

  private static void assertBetweenValues(
      NavigableMap<Integer, YTRID> keyValues,
      Random random,
      boolean fromInclusive,
      boolean toInclusive) {
    for (int i = 0; i < 100; i++) {
      int upperBorder = keyValues.lastKey() + 5000;
      int fromKey;
      if (upperBorder > 0) {
        fromKey = random.nextInt(upperBorder);
      } else {
        fromKey = random.nextInt(Integer.MAX_VALUE - 1);
      }

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(fromKey);
        if (includedKey != null) {
          fromKey = includedKey;
        } else {
          fromKey = keyValues.floorKey(fromKey);
        }
      }

      int toKey = random.nextInt() + fromKey + 1;
      if (toKey < 0) {
        toKey = Integer.MAX_VALUE;
      }

      if (random.nextBoolean()) {
        Integer includedKey = keyValues.ceilingKey(toKey);
        if (includedKey != null) {
          toKey = includedKey;
        } else {
          toKey = keyValues.floorKey(toKey);
        }
      }

      if (fromKey > toKey) {
        toKey = fromKey;
      }

      int maxValuesToFetch = 10000;

      Collection<YTIdentifiable> orids =
          sbTree.getValuesBetween(fromKey, fromInclusive, toKey, toInclusive, maxValuesToFetch);
      Set<YTIdentifiable> result = new HashSet<>(orids);

      Iterator<YTRID> valuesIterator =
          keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).values().iterator();

      int fetchedValues = 0;
      while (valuesIterator.hasNext() && fetchedValues < maxValuesToFetch) {
        YTRID value = valuesIterator.next();
        Assert.assertTrue(result.remove(value));

        fetchedValues++;
      }

      if (valuesIterator.hasNext()) {
        Assert.assertEquals(fetchedValues, maxValuesToFetch);
      }

      Assert.assertEquals(result.size(), 0);
    }
  }
}
