/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.stream.Streams;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.comparator.AscComparator;
import com.orientechnologies.orient.core.index.comparator.DescComparator;
import com.orientechnologies.orient.core.index.iterator.PureTxMultiValueBetweenIndexBackwardSplititerator;
import com.orientechnologies.orient.core.index.iterator.PureTxMultiValueBetweenIndexForwardSpliterator;
import com.orientechnologies.orient.core.index.multivalue.MultiValuesTransformer;
import com.orientechnologies.orient.core.index.multivalue.OMultivalueEntityRemover;
import com.orientechnologies.orient.core.index.multivalue.OMultivalueIndexKeyUpdaterImpl;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Abstract index implementation that supports multi-values for the same key.
 */
public abstract class OIndexMultiValues extends OIndexAbstract {

  OIndexMultiValues(OIndexMetadata im, final OStorage storage) {
    super(im, storage);
  }

  @Deprecated
  @Override
  public Collection<YTRID> get(YTDatabaseSessionInternal session, Object key) {
    final List<YTRID> rids;
    try (Stream<YTRID> stream = getRids(session, key)) {
      rids = stream.collect(Collectors.toList());
    }
    return rids;
  }

  @Override
  public Stream<YTRID> getRidsIgnoreTx(YTDatabaseSessionInternal session, Object key) {
    final Object collatedKey = getCollatingValue(key);
    Stream<YTRID> backedStream;
    acquireSharedLock();
    try {
      Stream<YTRID> stream;
      while (true) {
        try {
          if (apiVersion == 0) {
            //noinspection unchecked
            final Collection<YTRID> values =
                (Collection<YTRID>) storage.getIndexValue(session, indexId, collatedKey);
            if (values != null) {
              //noinspection resource
              stream = values.stream();
            } else {
              //noinspection resource
              stream = Stream.empty();
            }
          } else if (apiVersion == 1) {
            //noinspection resource
            stream = storage.getIndexValues(indexId, collatedKey);
          } else {
            throw new IllegalStateException("Invalid version of index API - " + apiVersion);
          }
          backedStream = IndexStreamSecurityDecorator.decorateRidStream(this, stream);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
    return backedStream;
  }

  @Override
  public Stream<YTRID> getRids(YTDatabaseSessionInternal session, Object key) {
    final Object collatedKey = getCollatingValue(key);
    Stream<YTRID> backedStream = getRidsIgnoreTx(session, key);
    final OTransactionIndexChanges indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return backedStream;
    }
    Set<YTIdentifiable> txChanges = calculateTxValue(collatedKey, indexChanges);
    if (txChanges == null) {
      txChanges = Collections.emptySet();
    }
    return IndexStreamSecurityDecorator.decorateRidStream(
        this,
        Stream.concat(
            backedStream
                .map((rid) -> calculateTxIndexEntry(collatedKey, rid, indexChanges))
                .filter(Objects::nonNull)
                .map((pair) -> pair.second),
            txChanges.stream().map(YTIdentifiable::getIdentity)));
  }

  public OIndexMultiValues put(YTDatabaseSessionInternal session, Object key,
      final YTIdentifiable singleValue) {
    final YTRID rid = singleValue.getIdentity();

    if (!rid.isValid()) {
      if (singleValue instanceof YTRecord) {
        // EARLY SAVE IT
        ((YTRecord) singleValue).save();
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }

    key = getCollatingValue(key);

    OTransaction singleTx = session.getTransaction();
    singleTx.addIndexEntry(
        this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, singleValue);
    return this;
  }

  @Override
  public void doPut(YTDatabaseSessionInternal session, OAbstractPaginatedStorage storage,
      Object key,
      YTRID rid)
      throws OInvalidIndexEngineIdException {
    if (apiVersion == 0) {
      doPutV0(session, indexId, storage, im.getValueContainerAlgorithm(), getName(), key, rid);
    } else if (apiVersion == 1) {
      doPutV1(storage, indexId, key, rid);
    } else {
      throw new IllegalStateException("Invalid API version, " + apiVersion);
    }
  }

  @Override
  public boolean isNativeTxSupported() {
    return true;
  }

  private static void doPutV0(
      YTDatabaseSessionInternal session, final int indexId,
      final OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm,
      String indexName,
      Object key,
      YTRID identity)
      throws OInvalidIndexEngineIdException {
    int binaryFormatVersion = storage.getConfiguration().getBinaryFormatVersion();
    final OIndexKeyUpdater<Object> creator =
        new OMultivalueIndexKeyUpdaterImpl(
            identity, valueContainerAlgorithm, binaryFormatVersion, indexName);

    storage.updateIndexEntry(session, indexId, key, creator);
  }

  private static void doPutV1(
      OAbstractPaginatedStorage storage, int indexId, Object key, YTRID identity)
      throws OInvalidIndexEngineIdException {
    storage.putRidIndexEntry(indexId, key, identity);
  }

  @Override
  public boolean remove(YTDatabaseSessionInternal session, Object key, final YTIdentifiable value) {
    key = getCollatingValue(key);
    session.getTransaction().addIndexEntry(this, super.getName(), OPERATION.REMOVE, key, value);
    return true;
  }

  @Override
  public boolean doRemove(YTDatabaseSessionInternal session, OAbstractPaginatedStorage storage,
      Object key, YTRID rid)
      throws OInvalidIndexEngineIdException {
    if (apiVersion == 0) {
      return doRemoveV0(session, indexId, storage, key, rid);
    }

    if (apiVersion == 1) {
      return doRemoveV1(indexId, storage, key, rid);
    }

    throw new IllegalStateException("Invalid API version, " + apiVersion);
  }

  private static boolean doRemoveV0(
      YTDatabaseSessionInternal session, int indexId, OAbstractPaginatedStorage storage, Object key,
      YTIdentifiable value)
      throws OInvalidIndexEngineIdException {
    Set<YTIdentifiable> values;
    //noinspection unchecked
    values = (Set<YTIdentifiable>) storage.getIndexValue(session, indexId, key);

    if (values == null) {
      return false;
    }

    final OModifiableBoolean removed = new OModifiableBoolean(false);
    final OIndexKeyUpdater<Object> creator = new OMultivalueEntityRemover(value, removed);
    storage.updateIndexEntry(session, indexId, key, creator);

    return removed.getValue();
  }

  private static boolean doRemoveV1(
      int indexId, OAbstractPaginatedStorage storage, Object key, YTIdentifiable value)
      throws OInvalidIndexEngineIdException {
    return storage.removeRidIndexEntry(indexId, key, value.getIdentity());
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> streamEntriesBetween(
      YTDatabaseSessionInternal session, Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);
    Stream<ORawPair<Object, YTRID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesBetween(session,
                      indexId,
                      fromKey,
                      fromInclusive,
                      toKey,
                      toInclusive,
                      ascOrder, MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    final OTransactionIndexChanges indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, YTRID>> txStream;
    if (ascOrder) {
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    } else {
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexBackwardSplititerator(
                  this, fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> streamEntriesMajor(
      YTDatabaseSessionInternal session, Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    Stream<ORawPair<Object, YTRID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesMajor(
                      indexId, fromKey, fromInclusive, ascOrder, MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    final OTransactionIndexChanges indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, YTRID>> txStream;

    final Object lastKey = indexChanges.getLastKey();
    if (ascOrder) {
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    } else {
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexBackwardSplititerator(
                  this, fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> streamEntriesMinor(
      YTDatabaseSessionInternal session, Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);
    Stream<ORawPair<Object, YTRID>> stream;

    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesMinor(
                      indexId, toKey, toInclusive, ascOrder, MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    final OTransactionIndexChanges indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, YTRID>> txStream;

    final Object firstKey = indexChanges.getFirstKey();
    if (ascOrder) {
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexForwardSpliterator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    } else {
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexBackwardSplititerator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> streamEntries(YTDatabaseSessionInternal session,
      Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<>(keys);
    final Comparator<Object> comparator;
    if (ascSortOrder) {
      comparator = ODefaultComparator.INSTANCE;
    } else {
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);
    }

    sortedKeys.sort(comparator);

    Stream<ORawPair<Object, YTRID>> stream =
        IndexStreamSecurityDecorator.decorateStream(
            this, sortedKeys.stream().flatMap(key1 -> streamForKey(session, key1)));

    final OTransactionIndexChanges indexChanges = session.getTransaction()
        .getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    Comparator<ORawPair<Object, YTRID>> keyComparator;
    if (ascSortOrder) {
      keyComparator = AscComparator.INSTANCE;
    } else {
      keyComparator = DescComparator.INSTANCE;
    }

    final Stream<ORawPair<Object, YTRID>> txStream =
        keys.stream()
            .flatMap((key) -> txStramForKey(indexChanges, key))
            .filter(Objects::nonNull)
            .sorted(keyComparator);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascSortOrder));
  }

  private Stream<ORawPair<Object, YTRID>> txStramForKey(
      final OTransactionIndexChanges indexChanges, Object key) {
    final Set<YTIdentifiable> result = calculateTxValue(getCollatingValue(key), indexChanges);
    if (result != null) {
      return result.stream()
          .map((rid) -> new ORawPair<>(getCollatingValue(key), rid.getIdentity()));
    }
    return null;
  }

  private Stream<ORawPair<Object, YTRID>> streamForKey(YTDatabaseSessionInternal session,
      Object key) {
    key = getCollatingValue(key);

    final Object entryKey = key;
    acquireSharedLock();
    try {
      while (true) {
        try {
          if (apiVersion == 0) {
            //noinspection unchecked,resource
            return Optional.ofNullable(
                    (Collection<YTRID>) storage.getIndexValue(session, indexId, key))
                .map((rids) -> rids.stream().map((rid) -> new ORawPair<>(entryKey, rid)))
                .orElse(Stream.empty());
          } else if (apiVersion == 1) {
            //noinspection resource
            return storage.getIndexValues(indexId, key).map((rid) -> new ORawPair<>(entryKey, rid));
          } else {
            throw new IllegalStateException("Invalid version of index API - " + apiVersion);
          }
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
  }

  public static Set<YTIdentifiable> calculateTxValue(
      final Object key, OTransactionIndexChanges indexChanges) {
    final List<YTIdentifiable> result = new ArrayList<>();
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      return null;
    }

    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.REMOVE) {
        if (entry.getValue() == null) {
          result.clear();
        } else {
          result.remove(entry.getValue());
        }
      } else {
        result.add(entry.getValue());
      }
    }

    if (result.isEmpty()) {
      return null;
    }

    return new HashSet<>(result);
  }

  public long size(YTDatabaseSessionInternal session) {
    acquireSharedLock();
    long tot;
    try {
      while (true) {
        try {
          tot = storage.getIndexSize(indexId, MultiValuesTransformer.INSTANCE);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    final OTransactionIndexChanges indexChanges =
        session.getTransaction().getIndexChanges(getName());
    if (indexChanges != null) {
      try (Stream<ORawPair<Object, YTRID>> stream = stream(session)) {
        return stream.count();
      }
    }

    return tot;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> stream(YTDatabaseSessionInternal session) {
    Stream<ORawPair<Object, YTRID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this, storage.getIndexStream(indexId, MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }

    final OTransactionIndexChanges indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, YTRID>> txStream =
        StreamSupport.stream(
            new PureTxMultiValueBetweenIndexForwardSpliterator(
                this, null, true, null, true, indexChanges),
            false);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, true));
  }

  private Stream<ORawPair<Object, YTRID>> mergeTxAndBackedStreams(
      OTransactionIndexChanges indexChanges,
      Stream<ORawPair<Object, YTRID>> txStream,
      Stream<ORawPair<Object, YTRID>> backedStream,
      boolean ascOrder) {
    Comparator<ORawPair<Object, YTRID>> keyComparator;
    if (ascOrder) {
      keyComparator = AscComparator.INSTANCE;
    } else {
      keyComparator = DescComparator.INSTANCE;
    }
    return Streams.mergeSortedSpliterators(
        txStream,
        backedStream
            .map((entry) -> calculateTxIndexEntry(entry.first, entry.second, indexChanges))
            .filter(Objects::nonNull),
        keyComparator);
  }

  private ORawPair<Object, YTRID> calculateTxIndexEntry(
      Object key, final YTRID backendValue, OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      return new ORawPair<>(key, backendValue);
    }

    int putCounter = 1;
    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.PUT && entry.getValue().equals(backendValue)) {
        putCounter++;
      } else if (entry.getOperation() == OPERATION.REMOVE) {
        if (entry.getValue() == null) {
          putCounter = 0;
        } else if (entry.getValue().equals(backendValue) && putCounter > 0) {
          putCounter--;
        }
      }
    }

    if (putCounter <= 0) {
      return null;
    }

    return new ORawPair<>(key, backendValue);
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> descStream(YTDatabaseSessionInternal session) {
    Stream<ORawPair<Object, YTRID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this, storage.getIndexDescStream(indexId, MultiValuesTransformer.INSTANCE));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    final OTransactionIndexChanges indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, YTRID>> txStream =
        StreamSupport.stream(
            new PureTxMultiValueBetweenIndexBackwardSplititerator(
                this, null, true, null, true, indexChanges),
            false);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, false));
  }
}
