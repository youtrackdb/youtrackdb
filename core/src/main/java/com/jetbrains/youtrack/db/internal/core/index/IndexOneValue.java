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

import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.comparator.DefaultComparator;
import com.jetbrains.youtrack.db.internal.common.stream.Streams;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.InvalidIndexEngineIdException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.comparator.AscComparator;
import com.jetbrains.youtrack.db.internal.core.index.comparator.DescComparator;
import com.jetbrains.youtrack.db.internal.core.index.iterator.PureTxBetweenIndexBackwardSpliterator;
import com.jetbrains.youtrack.db.internal.core.index.iterator.PureTxBetweenIndexForwardSpliterator;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges.OPERATION;
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
public abstract class IndexOneValue extends IndexAbstract {

  public IndexOneValue(IndexMetadata im, final Storage storage) {
    super(im, storage);
  }

  @Deprecated
  @Override
  public Object get(DatabaseSessionInternal session, Object key) {
    final Iterator<RID> iterator;
    try (var stream = getRids(session, key)) {
      iterator = stream.iterator();
      if (iterator.hasNext()) {
        return iterator.next();
      }
    }

    return null;
  }

  @Override
  public Stream<RID> getRidsIgnoreTx(DatabaseSessionInternal session, Object key) {
    key = getCollatingValue(key);

    acquireSharedLock();
    Stream<RID> stream;
    try {
      while (true) {
        try {
          if (apiVersion == 0) {
            final var rid = (RID) storage.getIndexValue(session, indexId, key);
            stream = Stream.ofNullable(rid);
          } else if (apiVersion == 1) {
            stream = storage.getIndexValues(indexId, key);
          } else {
            throw new IllegalStateException("Unknown version of index API " + apiVersion);
          }
          stream = IndexStreamSecurityDecorator.decorateRidStream(this, stream, session);
          break;
        } catch (InvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
    return stream;
  }

  @Override
  public Stream<RID> getRids(DatabaseSessionInternal session, Object key) {
    key = getCollatingValue(key);

    var stream = getRidsIgnoreTx(session, key);

    final var indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    RID rid;
    if (!indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      //noinspection resource
      rid = stream.findFirst().orElse(null);
    } else {
      rid = null;
    }

    final var txIndexEntry = calculateTxIndexEntry(key, rid, indexChanges);
    if (txIndexEntry == null) {
      return Stream.empty();
    }

    return IndexStreamSecurityDecorator.decorateRidStream(this, Stream.of(txIndexEntry.second),
        session);
  }

  @Override
  public Stream<RawPair<Object, RID>> streamEntries(DatabaseSessionInternal session,
      Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<>(keys);
    final Comparator<Object> comparator;

    if (ascSortOrder) {
      comparator = DefaultComparator.INSTANCE;
    } else {
      comparator = Collections.reverseOrder(DefaultComparator.INSTANCE);
    }

    sortedKeys.sort(comparator);

    //noinspection resource
    var stream =
        IndexStreamSecurityDecorator.decorateStream(
            this,
            sortedKeys.stream()
                .flatMap(
                    (key) -> {
                      final var collatedKey = getCollatingValue(key);

                      acquireSharedLock();
                      try {
                        while (true) {
                          try {
                            if (apiVersion == 0) {
                              final var rid = (RID) storage.getIndexValue(session, indexId,
                                  collatedKey);
                              if (rid == null) {
                                return Stream.empty();
                              }
                              return Stream.of(new RawPair<>(collatedKey, rid));
                            } else if (apiVersion == 1) {
                              return storage
                                  .getIndexValues(indexId, collatedKey)
                                  .map((rid) -> new RawPair<>(collatedKey, rid));
                            } else {
                              throw new IllegalStateException(
                                  "Invalid version of index API - " + apiVersion);
                            }
                          } catch (InvalidIndexEngineIdException ignore) {
                            doReloadIndexEngine();
                          }
                        }
                      } finally {
                        releaseSharedLock();
                      }
                    })
                .filter(Objects::nonNull), session);
    final var indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }
    Comparator<RawPair<Object, RID>> keyComparator;
    if (ascSortOrder) {
      keyComparator = AscComparator.INSTANCE;
    } else {
      keyComparator = DescComparator.INSTANCE;
    }

    @SuppressWarnings("resource") final var txStream =
        keys.stream()
            .map((key) -> calculateTxIndexEntry(getCollatingValue(key), null, indexChanges))
            .filter(Objects::nonNull)
            .sorted(keyComparator);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream, session);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascSortOrder), session);
  }

  @Override
  public Stream<RawPair<Object, RID>> streamEntriesBetween(
      DatabaseSessionInternal session, Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);
    Stream<RawPair<Object, RID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesBetween(session,
                      indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null),
                  session);
          break;
        } catch (InvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    final var indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<RawPair<Object, RID>> txStream;
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
      return IndexStreamSecurityDecorator.decorateStream(this, txStream, session);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder), session);
  }

  @Override
  public Stream<RawPair<Object, RID>> streamEntriesMajor(
      DatabaseSessionInternal session, Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    Stream<RawPair<Object, RID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesMajor(
                      indexId, fromKey, fromInclusive, ascOrder, null), session);
          break;
        } catch (InvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    final var indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    fromKey = getCollatingValue(fromKey);

    final Stream<RawPair<Object, RID>> txStream;

    final var lastKey = indexChanges.getLastKey();
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
      return IndexStreamSecurityDecorator.decorateStream(this, txStream, session);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder), session);
  }

  @Override
  public Stream<RawPair<Object, RID>> streamEntriesMinor(
      DatabaseSessionInternal session, Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);
    Stream<RawPair<Object, RID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null),
                  session);
          break;
        } catch (InvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
    final var indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    toKey = getCollatingValue(toKey);

    final Stream<RawPair<Object, RID>> txStream;

    final var firstKey = indexChanges.getFirstKey();
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
      return IndexStreamSecurityDecorator.decorateStream(this, txStream, session);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder), session);
  }

  public long size(DatabaseSessionInternal session) {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.getIndexSize(indexId, null);
        } catch (InvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<RawPair<Object, RID>> stream(DatabaseSessionInternal session) {
    Stream<RawPair<Object, RID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this, storage.getIndexStream(indexId, null), session);
          break;
        } catch (InvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    final var indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final var txStream =
        StreamSupport.stream(
            new PureTxBetweenIndexForwardSpliterator(this, null, true, null, true, indexChanges),
            false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream, session);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, true), session);
  }

  @Override
  public Stream<RawPair<Object, RID>> descStream(DatabaseSessionInternal session) {
    Stream<RawPair<Object, RID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this, storage.getIndexDescStream(indexId, null), session);
          break;
        } catch (InvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }

    final var indexChanges =
        session.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final var txStream =
        StreamSupport.stream(
            new PureTxBetweenIndexBackwardSpliterator(this, null, true, null, true, indexChanges),
            false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream, session);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, false), session);
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  public RawPair<Object, RID> calculateTxIndexEntry(
      Object key, final RID backendValue, final FrontendTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    var result = backendValue;
    final var changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      if (backendValue == null) {
        return null;
      } else {
        return new RawPair<>(key, backendValue);
      }
    }

    for (var entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.REMOVE) {
        result = null;
      } else if (entry.getOperation() == OPERATION.PUT) {
        result = entry.getValue().getIdentity();
      }
    }

    if (result == null) {
      return null;
    }

    return new RawPair<>(key, result);
  }

  private Stream<RawPair<Object, RID>> mergeTxAndBackedStreams(
      FrontendTransactionIndexChanges indexChanges,
      Stream<RawPair<Object, RID>> txStream,
      Stream<RawPair<Object, RID>> backedStream,
      boolean ascSortOrder) {
    Comparator<RawPair<Object, RID>> comparator;
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
  public IndexOneValue put(DatabaseSessionInternal db, Object key,
      final Identifiable value) {
    final var rid = (RecordId) value.getIdentity();

    if (!rid.isValid()) {
      if (value instanceof DBRecord) {
        // EARLY SAVE IT
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }
    key = getCollatingValue(key);

    var singleTx = db.getTransaction();
    singleTx.addIndexEntry(
        this, super.getName(), FrontendTransactionIndexChanges.OPERATION.PUT, key,
        value.getIdentity());
    return this;
  }
}
