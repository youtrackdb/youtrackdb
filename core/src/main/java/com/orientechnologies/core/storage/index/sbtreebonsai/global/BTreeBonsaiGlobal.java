package com.orientechnologies.core.storage.index.sbtreebonsai.global;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.ORawPairObjectInteger;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.core.storage.index.sbtreebonsai.global.btree.BTree;
import com.orientechnologies.core.storage.index.sbtreebonsai.global.btree.EdgeKey;
import com.orientechnologies.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;

public class BTreeBonsaiGlobal implements OSBTreeBonsai<YTIdentifiable, Integer> {

  private final BTree bTree;
  private final int intFileId;
  private final long ridBagId;

  private final OBinarySerializer<YTIdentifiable> keySerializer;
  private final OBinarySerializer<Integer> valueSerializer;

  public BTreeBonsaiGlobal(
      final BTree bTree,
      final int intFileId,
      final long ridBagId,
      OBinarySerializer<YTIdentifiable> keySerializer,
      OBinarySerializer<Integer> valueSerializer) {
    this.bTree = bTree;
    this.intFileId = intFileId;
    this.ridBagId = ridBagId;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public OBonsaiCollectionPointer getCollectionPointer() {
    return new OBonsaiCollectionPointer(intFileId, getRootBucketPointer());
  }

  @Override
  public long getFileId() {
    return bTree.getFileId();
  }

  @Override
  public OBonsaiBucketPointer getRootBucketPointer() {
    return new OBonsaiBucketPointer(ridBagId, 0);
  }

  @Override
  public Integer get(YTIdentifiable key) {
    final YTRID rid = key.getIdentity();

    final int result;

    result = bTree.get(new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()));

    if (result < 0) {
      return null;
    }

    return result;
  }

  @Override
  public boolean put(OAtomicOperation atomicOperation, YTIdentifiable key, Integer value) {
    final YTRID rid = key.getIdentity();

    return bTree.put(
        atomicOperation,
        new EdgeKey(ridBagId, rid.getClusterId(), rid.getClusterPosition()),
        value);
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) {
    try (Stream<ORawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      final Iterator<ORawPairObjectInteger<EdgeKey>> iterator = stream.iterator();

      while (iterator.hasNext()) {
        final ORawPairObjectInteger<EdgeKey> entry = iterator.next();
        bTree.remove(atomicOperation, entry.first);
      }
    }
  }

  public boolean isEmpty() {
    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesMajor(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE), true, true)) {
      return stream.findAny().isEmpty();
    }
  }

  @Override
  public void delete(OAtomicOperation atomicOperation) {
    clear(atomicOperation);
  }

  @Override
  public Integer remove(OAtomicOperation atomicOperation, YTIdentifiable key) {
    final YTRID rid = key.getIdentity();
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
      YTIdentifiable key, boolean inclusive, int maxValuesToFetch) {
    final YTRID rid = key.getIdentity();
    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
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
      YTIdentifiable key, boolean inclusive,
      RangeResultListener<YTIdentifiable, Integer> listener) {
    final YTRID rid = key.getIdentity();
    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
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
      YTIdentifiable key, boolean inclusive, int maxValuesToFetch) {
    final YTRID rid = key.getIdentity();

    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
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
      YTIdentifiable key,
      boolean inclusive,
      boolean ascSortOrder,
      RangeResultListener<YTIdentifiable, Integer> listener) {
    final YTRID rid = key.getIdentity();
    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
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
      YTIdentifiable keyFrom,
      boolean fromInclusive,
      YTIdentifiable keyTo,
      boolean toInclusive,
      int maxValuesToFetch) {
    final YTRID ridFrom = keyFrom.getIdentity();
    final YTRID ridTo = keyTo.getIdentity();
    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
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
  public YTIdentifiable firstKey() {
    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      final Iterator<ORawPairObjectInteger<EdgeKey>> iterator = stream.iterator();
      if (iterator.hasNext()) {
        final ORawPairObjectInteger<EdgeKey> entry = iterator.next();
        return new YTRecordId(entry.first.targetCluster, entry.first.targetPosition);
      }
    }

    return null;
  }

  @Override
  public YTIdentifiable lastKey() {
    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            false)) {
      final Iterator<ORawPairObjectInteger<EdgeKey>> iterator = stream.iterator();
      if (iterator.hasNext()) {
        final ORawPairObjectInteger<EdgeKey> entry = iterator.next();
        return new YTRecordId(entry.first.targetCluster, entry.first.targetPosition);
      }
    }

    return null;
  }

  @Override
  public void loadEntriesBetween(
      YTIdentifiable keyFrom,
      boolean fromInclusive,
      YTIdentifiable keyTo,
      boolean toInclusive,
      RangeResultListener<YTIdentifiable, Integer> listener) {
    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
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
  public int getRealBagSize(Map<YTIdentifiable, Change> changes) {
    final Map<YTIdentifiable, Change> notAppliedChanges = new HashMap<>(changes);
    final OModifiableInteger size = new OModifiableInteger(0);

    try (final Stream<ORawPairObjectInteger<EdgeKey>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      forEachEntry(
          stream,
          entry -> {
            final YTRecordId rid =
                new YTRecordId(entry.first.targetCluster, entry.first.targetPosition);
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
  public OBinarySerializer<YTIdentifiable> getKeySerializer() {
    return keySerializer;
  }

  @Override
  public OBinarySerializer<Integer> getValueSerializer() {
    return valueSerializer;
  }

  private static void forEachEntry(
      final Stream<ORawPairObjectInteger<EdgeKey>> stream,
      final Function<ORawPairObjectInteger<EdgeKey>, Boolean> consumer) {

    boolean cont = true;

    final Iterator<ORawPairObjectInteger<EdgeKey>> iterator = stream.iterator();
    while (iterator.hasNext() && cont) {
      cont = consumer.apply(iterator.next());
    }
  }

  private static IntArrayList streamToList(
      final Stream<ORawPairObjectInteger<EdgeKey>> stream, int maxValuesToFetch) {
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
      final Stream<ORawPairObjectInteger<EdgeKey>> stream,
      final RangeResultListener<YTIdentifiable, Integer> listener) {
    forEachEntry(
        stream,
        entry ->
            listener.addResult(
                new Entry<YTIdentifiable, Integer>() {
                  @Override
                  public YTIdentifiable getKey() {
                    return new YTRecordId(entry.first.targetCluster, entry.first.targetPosition);
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
