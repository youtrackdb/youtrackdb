package com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.global;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.types.ModifiableInteger;
import com.jetbrains.youtrack.db.internal.common.util.RawPairObjectInteger;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.global.btree.BTree;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.global.btree.EdgeKey;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.BonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.SBTreeBonsai;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.Change;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;

public class BTreeBonsaiGlobal implements SBTreeBonsai<Identifiable, Integer> {

  private final BTree bTree;
  private final int intFileId;
  private final long ridBagId;

  private final BinarySerializer<Identifiable> keySerializer;
  private final BinarySerializer<Integer> valueSerializer;

  public BTreeBonsaiGlobal(
      final BTree bTree,
      final int intFileId,
      final long ridBagId,
      BinarySerializer<Identifiable> keySerializer,
      BinarySerializer<Integer> valueSerializer) {
    this.bTree = bTree;
    this.intFileId = intFileId;
    this.ridBagId = ridBagId;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public BonsaiCollectionPointer getCollectionPointer() {
    return new BonsaiCollectionPointer(intFileId, getRootBucketPointer());
  }

  @Override
  public long getFileId() {
    return bTree.getFileId();
  }

  @Override
  public BonsaiBucketPointer getRootBucketPointer() {
    return new BonsaiBucketPointer(ridBagId, 0);
  }

  @Override
  public Integer get(Identifiable key) {
    final RID rid = key.getIdentity();

    final int result;

    result = bTree.get(new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()));

    if (result < 0) {
      return null;
    }

    return result;
  }

  @Override
  public boolean put(AtomicOperation atomicOperation, Identifiable key, Integer value) {
    final RID rid = key.getIdentity();

    return bTree.put(
        atomicOperation,
        new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
        value);
  }

  @Override
  public void clear(AtomicOperation atomicOperation) {
    try (Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      final Iterator<RawPairObjectInteger<EdgeKey>> iterator = stream.iterator();

      while (iterator.hasNext()) {
        final RawPairObjectInteger<EdgeKey> entry = iterator.next();
        bTree.remove(atomicOperation, entry.first);
      }
    }
  }

  public boolean isEmpty() {
    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesMajor(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE), true, true)) {
      return stream.findAny().isEmpty();
    }
  }

  @Override
  public void delete(AtomicOperation atomicOperation) {
    clear(atomicOperation);
  }

  @Override
  public Integer remove(AtomicOperation atomicOperation, Identifiable key) {
    final RID rid = key.getIdentity();
    final int result;
    result =
        bTree.remove(
            atomicOperation, new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()));

    if (result < 0) {
      return null;
    }

    return result;
  }

  @Override
  public Collection<Integer> getValuesMinor(
      Identifiable key, boolean inclusive, int maxValuesToFetch) {
    final RID rid = key.getIdentity();
    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
            inclusive,
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            false)) {

      return streamToList(stream, maxValuesToFetch);
    }
  }

  @Override
  public void loadEntriesMinor(
      Identifiable key, boolean inclusive,
      RangeResultListener<Identifiable, Integer> listener) {
    final RID rid = key.getIdentity();
    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
            inclusive,
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            false)) {
      listenStream(stream, listener);
    }
  }

  @Override
  public Collection<Integer> getValuesMajor(
      Identifiable key, boolean inclusive, int maxValuesToFetch) {
    final RID rid = key.getIdentity();

    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      return streamToList(stream, maxValuesToFetch);
    }
  }

  @Override
  public void loadEntriesMajor(
      Identifiable key,
      boolean inclusive,
      boolean ascSortOrder,
      RangeResultListener<Identifiable, Integer> listener) {
    final RID rid = key.getIdentity();
    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
            inclusive,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      listenStream(stream, listener);
    }
  }

  @Override
  public Collection<Integer> getValuesBetween(
      Identifiable keyFrom,
      boolean fromInclusive,
      Identifiable keyTo,
      boolean toInclusive,
      int maxValuesToFetch) {
    final RID ridFrom = keyFrom.getIdentity();
    final RID ridTo = keyTo.getIdentity();
    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, ridFrom.getClusterId(), ridFrom.getClusterPosition()),
            fromInclusive,
            new EdgeKey(ridBagId, ridTo.getClusterId(), ridTo.getClusterPosition()),
            toInclusive,
            true)) {
      return streamToList(stream, maxValuesToFetch);
    }
  }

  @Override
  public Identifiable firstKey() {
    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      final Iterator<RawPairObjectInteger<EdgeKey>> iterator = stream.iterator();
      if (iterator.hasNext()) {
        final RawPairObjectInteger<EdgeKey> entry = iterator.next();
        return new RecordId(entry.first.targetCluster, entry.first.targetPosition);
      }
    }

    return null;
  }

  @Override
  public Identifiable lastKey() {
    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            false)) {
      final Iterator<RawPairObjectInteger<EdgeKey>> iterator = stream.iterator();
      if (iterator.hasNext()) {
        final RawPairObjectInteger<EdgeKey> entry = iterator.next();
        return new RecordId(entry.first.targetCluster, entry.first.targetPosition);
      }
    }

    return null;
  }

  @Override
  public void loadEntriesBetween(
      Identifiable keyFrom,
      boolean fromInclusive,
      Identifiable keyTo,
      boolean toInclusive,
      RangeResultListener<Identifiable, Integer> listener) {
    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            false)) {
      listenStream(stream, listener);
    }
  }

  @Override
  public int getRealBagSize(Map<Identifiable, Change> changes) {
    final Map<Identifiable, Change> notAppliedChanges = new HashMap<>(changes);
    final ModifiableInteger size = new ModifiableInteger(0);

    try (final Stream<RawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      forEachEntry(
          stream,
          entry -> {
            final RecordId rid =
                new RecordId(entry.first.targetCluster, entry.first.targetPosition);
            final Change change = notAppliedChanges.remove(rid);
            final int result;

            final Integer treeValue = entry.second;
            if (change == null) {
              result = treeValue;
            } else {
              result = change.applyTo(treeValue);
            }

            size.increment(result);
            return true;
          });
    }

    for (final Change change : notAppliedChanges.values()) {
      final int result = change.applyTo(0);
      size.increment(result);
    }

    return size.value;
  }

  @Override
  public BinarySerializer<Identifiable> getKeySerializer() {
    return keySerializer;
  }

  @Override
  public BinarySerializer<Integer> getValueSerializer() {
    return valueSerializer;
  }

  private static void forEachEntry(
      final Stream<RawPairObjectInteger<EdgeKey>> stream,
      final Function<RawPairObjectInteger<EdgeKey>, Boolean> consumer) {

    boolean cont = true;

    final Iterator<RawPairObjectInteger<EdgeKey>> iterator = stream.iterator();
    while (iterator.hasNext() && cont) {
      cont = consumer.apply(iterator.next());
    }
  }

  private static IntArrayList streamToList(
      final Stream<RawPairObjectInteger<EdgeKey>> stream, int maxValuesToFetch) {
    if (maxValuesToFetch < 0) {
      maxValuesToFetch = Integer.MAX_VALUE;
    }

    final IntArrayList result = new IntArrayList(Math.max(8, maxValuesToFetch));

    final int limit = maxValuesToFetch;
    forEachEntry(
        stream,
        entry -> {
          result.add(entry.second);
          return result.size() < limit;
        });

    return result;
  }

  private static void listenStream(
      final Stream<RawPairObjectInteger<EdgeKey>> stream,
      final RangeResultListener<Identifiable, Integer> listener) {
    forEachEntry(
        stream,
        entry ->
            listener.addResult(
                new Entry<Identifiable, Integer>() {
                  @Override
                  public Identifiable getKey() {
                    return new RecordId(entry.first.targetCluster, entry.first.targetPosition);
                  }

                  @Override
                  public Integer getValue() {
                    return entry.second;
                  }

                  @Override
                  public Integer setValue(Integer value) {
                    throw new UnsupportedOperationException();
                  }
                }));
  }
}
