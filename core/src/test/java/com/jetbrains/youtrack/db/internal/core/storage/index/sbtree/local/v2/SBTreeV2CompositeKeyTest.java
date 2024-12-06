package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.v2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.index.CompositeKeySerializer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 15.08.13
 */
public class SBTreeV2CompositeKeyTest extends DbTestBase {

  private SBTreeV2<CompositeKey, Identifiable> localSBTree;
  private AtomicOperationsManager atomicOperationsManager;

  @Before
  public void beforeMethod() throws Exception {
    atomicOperationsManager =
        ((AbstractPaginatedStorage) db.getStorage()).getAtomicOperationsManager();
    //noinspection deprecation
    localSBTree =
        new SBTreeV2<>(
            "localSBTreeCompositeKeyTest",
            ".sbt",
            ".nbt",
            (AbstractPaginatedStorage) db.getStorage());
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            localSBTree.create(
                atomicOperation,
                CompositeKeySerializer.INSTANCE,
                LinkSerializer.INSTANCE,
                null,
                2,
                false,
                null));

    for (double i = 1; i < 4; i++) {
      for (double j = 1; j < 10; j++) {
        final CompositeKey compositeKey = new CompositeKey();
        compositeKey.addKey(i);
        compositeKey.addKey(j);
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                localSBTree.put(
                    atomicOperation,
                    compositeKey,
                    new RecordId(
                        ((Double) compositeKey.getKeys().get(0)).intValue(),
                        ((Double) compositeKey.getKeys().get(1)).longValue())));
      }
    }
  }

  @After
  public void afterClass() throws Exception {
    try (Stream<CompositeKey> keyStream = localSBTree.keyStream()) {
      keyStream.forEach(
          (key) -> {
            try {
              atomicOperationsManager.executeInsideAtomicOperation(
                  null, atomicOperation -> localSBTree.remove(atomicOperation, key));
            } catch (IOException e) {
              throw new IllegalStateException(e);
            }
          });
    }

    if (localSBTree.isNullPointerSupport()) {
      atomicOperationsManager.executeInsideAtomicOperation(
          null, atomicOperation -> localSBTree.remove(atomicOperation, null));
    }

    atomicOperationsManager.executeInsideAtomicOperation(
        null, atomicOperation -> localSBTree.delete(atomicOperation));
  }

  @Test
  public void testIterateBetweenValuesInclusive() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), true, true);

    Set<RID> orids = extractRids(stream);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 18);
    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateBetweenValuesFromInclusive() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), false, true);

    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 9);

    for (int j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new RecordId(2, j)));
    }

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new RecordId(2, j)));
    }
  }

  @Test
  public void testIterateBetweenValuesToInclusive() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), true, true);
    Set<RID> orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(3, i)));
    }

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(3, i)));
    }
  }

  @Test
  public void testIterateEntriesNonInclusive() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), false, true);
    Set<RID> orids = extractRids(stream);

    assertEquals(orids.size(), 0);

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0), false, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 0);

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(1.0), false, compositeKey(3.0), false, true);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(2, i)));
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(1.0), false, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(2, i)));
    }
  }

  @Test
  public void testIterateBetweenValuesInclusivePartialKey() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), true, true);
    Set<RID> orids = extractRids(stream);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 4) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 4) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateBetweenValuesFromInclusivePartialKey() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), false, true);

    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 6);

    for (int j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new RecordId(2, j)));
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 6);

    for (int j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new RecordId(2, j)));
    }
  }

  @Test
  public void testIterateBetweenValuesToInclusivePartialKey() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), true, true);

    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 14);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 4) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), true, false);
    orids = extractRids(stream);
    assertEquals(orids.size(), 14);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 4) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateBetweenValuesNonInclusivePartial() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), false, true);

    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 5);

    for (int i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(2, i)));
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 5);

    for (int i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(2, i)));
    }
  }

  @Test
  public void testIterateValuesMajorInclusivePartial() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0), true, true);
    Set<RID> orids = extractRids(stream);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 18);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateMajorNonInclusivePartial() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0), false, true);
    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(3, i)));
    }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 9);

    for (int i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(3, i)));
    }
  }

  @Test
  public void testIterateValuesMajorInclusive() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), true, true);
    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 16);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 3) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 16);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j < 3) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMajorNonInclusive() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), false, true);
    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 15);

    for (int i = 2; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMinorInclusivePartial() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0), true, true);
    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 27);

    for (int i = 1; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 27);

    for (int i = 1; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMinorNonInclusivePartial() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0), false, true);
    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 18);

    for (int i = 1; i < 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 18);

    for (int i = 1; i < 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMinorInclusive() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), true, true);
    Set<RID> orids = extractRids(stream);
    assertEquals(orids.size(), 20);

    for (int i = 1; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 3 && j > 2) {
          continue;
        }

        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), true, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 20);

    for (int i = 1; i <= 3; i++) {
      for (int j = 1; j <= 9; j++) {
        if (i == 3 && j > 2) {
          continue;
        }

        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMinorNonInclusive() {
    Stream<RawPair<CompositeKey, Identifiable>> stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false, true);
    Set<RID> orids = extractRids(stream);

    assertEquals(orids.size(), 19);

    for (int i = 1; i < 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false, false);
    orids = extractRids(stream);

    assertEquals(orids.size(), 19);

    for (int i = 1; i < 3; i++) {
      for (int j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  private static CompositeKey compositeKey(Comparable<?>... params) {
    return new CompositeKey(Arrays.asList(params));
  }

  private static Set<RID> extractRids(Stream<RawPair<CompositeKey, Identifiable>> stream) {
    return stream.map((entry) -> entry.second.getIdentity()).collect(Collectors.toSet());
  }
}
