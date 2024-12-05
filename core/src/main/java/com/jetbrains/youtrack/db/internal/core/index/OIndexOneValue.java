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
package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.common.comparator.ODefaultComparator;
import com.jetbrains.youtrack.db.internal.common.stream.Streams;
import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.OInvalidIndexEngineIdException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.comparator.AscComparator;
import com.jetbrains.youtrack.db.internal.core.index.comparator.DescComparator;
import com.jetbrains.youtrack.db.internal.core.index.iterator.PureTxBetweenIndexBackwardSpliterator;
import com.jetbrains.youtrack.db.internal.core.index.iterator.PureTxBetweenIndexForwardSpliterator;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.storage.OStorage;
import com.jetbrains.youtrack.db.internal.core.tx.OTransaction;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChanges.OPERATION;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChangesPerKey;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Abstract Index implementation that allows only one value for a key.
 */
public abstract class OIndexOneValue extends OIndexAbstract {

  public OIndexOneValue(OIndexMetadata im, final OStorage storage) {
    super(im, storage);
  }

  @Deprecated
  @Override
  public Object get(YTDatabaseSessionInternal session, Object key) {
    final Iterator<YTRID> iterator;
    try (Stream<YTRID> stream = getRids(session, key)) {
      iterator = stream.iterator();
      if (iterator.hasNext()) {
        return iterator.next();
      }
    }

    return null;
  }

  @Override
  public Stream<YTRID> getRidsIgnoreTx(YTDatabaseSessionInternal session, Object key) {
    key = getCollatingValue(key);

    acquireSharedLock();
    Stream<YTRID> stream;
    try {
      while (true) {
        try {
          if (apiVersion == 0) {
            final YTRID rid = (YTRID) storage.getIndexValue(session, indexId, key);
            stream = Stream.ofNullable(rid);
          } else if (apiVersion == 1) {
            stream = storage.getIndexValues(indexId, key);
          } else {
            throw new IllegalStateException("Unknown version of index API " + apiVersion);
          }
          stream = IndexStreamSecurityDecorator.decorateRidStream(this, stream);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
    return stream;
  }

  @Override
  public Stream<YTRID> getRids(YTDatabaseSessionInternal session, Object key) {
    key = getCollatingValue(key);

    Stream<YTRID> stream = getRidsIgnoreTx(session, key);

    final OTransactionIndexChanges indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    YTRID rid;
    if (!indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      //noinspection resource
      rid = stream.findFirst().orElse(null);
    } else {
      rid = null;
    }

    final ORawPair<Object, YTRID> txIndexEntry = calculateTxIndexEntry(key, rid, indexChanges);
    if (txIndexEntry == null) {
      return Stream.empty();
    }

    return IndexStreamSecurityDecorator.decorateRidStream(this, Stream.of(txIndexEntry.second));
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

    //noinspection resource
    Stream<ORawPair<Object, YTRID>> stream =
        IndexStreamSecurityDecorator.decorateStream(
            this,
            sortedKeys.stream()
                .flatMap(
                    (key) -> {
                      final Object collatedKey = getCollatingValue(key);

                      acquireSharedLock();
                      try {
                        while (true) {
                          try {
                            if (apiVersion == 0) {
                              final YTRID rid = (YTRID) storage.getIndexValue(session, indexId,
                                  collatedKey);
                              if (rid == null) {
                                return Stream.empty();
                              }
                              return Stream.of(new ORawPair<>(collatedKey, rid));
                            } else if (apiVersion == 1) {
                              return storage
                                  .getIndexValues(indexId, collatedKey)
                                  .map((rid) -> new ORawPair<>(collatedKey, rid));
                            } else {
                              throw new IllegalStateException(
                                  "Invalid version of index API - " + apiVersion);
                            }
                          } catch (OInvalidIndexEngineIdException ignore) {
                            doReloadIndexEngine();
                          }
                        }
                      } finally {
                        releaseSharedLock();
                      }
                    })
                .filter(Objects::nonNull));
    final OTransactionIndexChanges indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }
    Comparator<ORawPair<Object, YTRID>> keyComparator;
    if (ascSortOrder) {
      keyComparator = AscComparator.INSTANCE;
    } else {
      keyComparator = DescComparator.INSTANCE;
    }

    @SuppressWarnings("resource") final Stream<ORawPair<Object, YTRID>> txStream =
        keys.stream()
            .map((key) -> calculateTxIndexEntry(getCollatingValue(key), null, indexChanges))
            .filter(Objects::nonNull)
            .sorted(keyComparator);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascSortOrder));
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
                      indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null));
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
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexBackwardSpliterator(
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
                      indexId, fromKey, fromInclusive, ascOrder, null));
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

    fromKey = getCollatingValue(fromKey);

    final Stream<ORawPair<Object, YTRID>> txStream;

    final Object lastKey = indexChanges.getLastKey();
    if (ascOrder) {
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    } else {
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexBackwardSpliterator(
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
                  storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null));
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

    toKey = getCollatingValue(toKey);

    final Stream<ORawPair<Object, YTRID>> txStream;

    final Object firstKey = indexChanges.getFirstKey();
    if (ascOrder) {
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexForwardSpliterator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    } else {
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexBackwardSpliterator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  public long size(YTDatabaseSessionInternal session) {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.getIndexSize(indexId, null);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
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
                  this, storage.getIndexStream(indexId, null));
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
            new PureTxBetweenIndexForwardSpliterator(this, null, true, null, true, indexChanges),
            false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, true));
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
                  this, storage.getIndexDescStream(indexId, null));
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
            new PureTxBetweenIndexBackwardSpliterator(this, null, true, null, true, indexChanges),
            false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, false));
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  public ORawPair<Object, YTRID> calculateTxIndexEntry(
      Object key, final YTRID backendValue, final OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    YTRID result = backendValue;
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      if (backendValue == null) {
        return null;
      } else {
        return new ORawPair<>(key, backendValue);
      }
    }

    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.REMOVE) {
        result = null;
      } else if (entry.getOperation() == OPERATION.PUT) {
        result = entry.getValue().getIdentity();
      }
    }

    if (result == null) {
      return null;
    }

    return new ORawPair<>(key, result);
  }

  private Stream<ORawPair<Object, YTRID>> mergeTxAndBackedStreams(
      OTransactionIndexChanges indexChanges,
      Stream<ORawPair<Object, YTRID>> txStream,
      Stream<ORawPair<Object, YTRID>> backedStream,
      boolean ascSortOrder) {
    Comparator<ORawPair<Object, YTRID>> comparator;
    if (ascSortOrder) {
      comparator = AscComparator.INSTANCE;
    } else {
      comparator = DescComparator.INSTANCE;
    }

    return Streams.mergeSortedSpliterators(
        txStream,
        backedStream
            .map(
                (entry) ->
                    calculateTxIndexEntry(
                        getCollatingValue(entry.first), entry.second, indexChanges))
            .filter(Objects::nonNull),
        comparator);
  }

  @Override
  public OIndexOneValue put(YTDatabaseSessionInternal session, Object key,
      final YTIdentifiable value) {
    final YTRID rid = value.getIdentity();

    if (!rid.isValid()) {
      if (value instanceof Record) {
        // EARLY SAVE IT
        ((Record) value).save();
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }
    key = getCollatingValue(key);

    OTransaction singleTx = session.getTransaction();
    singleTx.addIndexEntry(
        this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, value.getIdentity());
    return this;
  }
}
