package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.v2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
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

  private SBTreeV2<CompositeKey, RID> localSBTree;
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
                false
            ));

    for (double i = 1; i < 4; i++) {
      for (double j = 1; j < 10; j++) {
        final var compositeKey = new CompositeKey();
        compositeKey.addKey(i);
        compositeKey.addKey(j);
        atomicOperationsManager.executeInsideAtomicOperation(
            null,
            atomicOperation ->
                localSBTree.put(
                    atomicOperation,
                    compositeKey,
                    new RecordId(
                        ((Double) compositeKey.getKeys().getFirst()).intValue(),
                        ((Double) compositeKey.getKeys().get(1)).longValue())));
      }
    }
  }

  @After
  public void afterClass() throws Exception {
    try (var keyStream = localSBTree.keyStream()) {
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
    var stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), true, true);

    var orids = extractRids(stream);

    assertEquals(18, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(18, orids.size());
    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateBetweenValuesFromInclusive() {
    var stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), false, true);

    var orids = extractRids(stream);
    assertEquals(9, orids.size());

    for (var j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new RecordId(2, j)));
    }

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), true, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(9, orids.size());

    for (var j = 1; j <= 9; j++) {
      assertTrue(orids.contains(new RecordId(2, j)));
    }
  }

  @Test
  public void testIterateBetweenValuesToInclusive() {
    var stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), true, true);
    var orids = extractRids(stream);

    assertEquals(9, orids.size());

    for (var i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(3, i)));
    }

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(9, orids.size());

    for (var i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(3, i)));
    }
  }

  @Test
  public void testIterateEntriesNonInclusive() {
    var stream =
        localSBTree.iterateEntriesBetween(compositeKey(2.0), false, compositeKey(3.0), false, true);
    var orids = extractRids(stream);

    assertEquals(0, orids.size());

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0), false, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(0, orids.size());

    stream =
        localSBTree.iterateEntriesBetween(compositeKey(1.0), false, compositeKey(3.0), false, true);
    orids = extractRids(stream);

    assertEquals(9, orids.size());

    for (var i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(2, i)));
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(1.0), false, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(9, orids.size());

    for (var i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(2, i)));
    }
  }

  @Test
  public void testIterateBetweenValuesInclusivePartialKey() {
    var stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), true, true);
    var orids = extractRids(stream);

    assertEquals(15, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
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

    assertEquals(15, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        if (i == 2 && j < 4) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateBetweenValuesFromInclusivePartialKey() {
    var stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), false, true);

    var orids = extractRids(stream);
    assertEquals(6, orids.size());

    for (var j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new RecordId(2, j)));
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), true, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(6, orids.size());

    for (var j = 4; j <= 9; j++) {
      assertTrue(orids.contains(new RecordId(2, j)));
    }
  }

  @Test
  public void testIterateBetweenValuesToInclusivePartialKey() {
    var stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), true, true);

    var orids = extractRids(stream);
    assertEquals(14, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
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
    assertEquals(14, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        if (i == 2 && j <= 4) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateBetweenValuesNonInclusivePartial() {
    var stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), false, true);

    var orids = extractRids(stream);
    assertEquals(5, orids.size());

    for (var i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(2, i)));
    }

    stream =
        localSBTree.iterateEntriesBetween(
            compositeKey(2.0, 4.0), false, compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(5, orids.size());

    for (var i = 5; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(2, i)));
    }
  }

  @Test
  public void testIterateValuesMajorInclusivePartial() {
    var stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0), true, true);
    var orids = extractRids(stream);

    assertEquals(18, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0), true, false);
    orids = extractRids(stream);

    assertEquals(18, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateMajorNonInclusivePartial() {
    var stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0), false, true);
    var orids = extractRids(stream);
    assertEquals(9, orids.size());

    for (var i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(3, i)));
    }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0), false, false);
    orids = extractRids(stream);

    assertEquals(9, orids.size());

    for (var i = 1; i <= 9; i++) {
      assertTrue(orids.contains(new RecordId(3, i)));
    }
  }

  @Test
  public void testIterateValuesMajorInclusive() {
    var stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), true, true);
    var orids = extractRids(stream);
    assertEquals(16, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        if (i == 2 && j < 3) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), true, false);
    orids = extractRids(stream);

    assertEquals(16, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        if (i == 2 && j < 3) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMajorNonInclusive() {
    var stream =
        localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), false, true);
    var orids = extractRids(stream);
    assertEquals(15, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMajor(compositeKey(2.0, 3.0), false, false);
    orids = extractRids(stream);

    assertEquals(15, orids.size());

    for (var i = 2; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        if (i == 2 && j <= 3) {
          continue;
        }
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMinorInclusivePartial() {
    var stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0), true, true);
    var orids = extractRids(stream);
    assertEquals(27, orids.size());

    for (var i = 1; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0), true, false);
    orids = extractRids(stream);

    assertEquals(27, orids.size());

    for (var i = 1; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMinorNonInclusivePartial() {
    var stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0), false, true);
    var orids = extractRids(stream);
    assertEquals(18, orids.size());

    for (var i = 1; i < 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0), false, false);
    orids = extractRids(stream);

    assertEquals(18, orids.size());

    for (var i = 1; i < 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMinorInclusive() {
    var stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), true, true);
    var orids = extractRids(stream);
    assertEquals(20, orids.size());

    for (var i = 1; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        if (i == 3 && j > 2) {
          continue;
        }

        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), true, false);
    orids = extractRids(stream);

    assertEquals(20, orids.size());

    for (var i = 1; i <= 3; i++) {
      for (var j = 1; j <= 9; j++) {
        if (i == 3 && j > 2) {
          continue;
        }

        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  @Test
  public void testIterateValuesMinorNonInclusive() {
    var stream =
        localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false, true);
    var orids = extractRids(stream);

    assertEquals(19, orids.size());

    for (var i = 1; i < 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }

    stream = localSBTree.iterateEntriesMinor(compositeKey(3.0, 2.0), false, false);
    orids = extractRids(stream);

    assertEquals(19, orids.size());

    for (var i = 1; i < 3; i++) {
      for (var j = 1; j <= 9; j++) {
        assertTrue(orids.contains(new RecordId(i, j)));
      }
    }
  }

  private static CompositeKey compositeKey(Comparable<?>... params) {
    return new CompositeKey(Arrays.asList(params));
  }

  private static Set<RID> extractRids(Stream<RawPair<CompositeKey, RID>> stream) {
    return stream.map((entry) -> entry.second).collect(Collectors.toSet());
  }
}
