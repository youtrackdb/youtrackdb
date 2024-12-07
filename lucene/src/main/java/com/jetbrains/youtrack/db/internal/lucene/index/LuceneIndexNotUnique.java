/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jetbrains.youtrack.db.internal.lucene.index;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.exception.InvalidIndexEngineIdException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.IndexAbstract;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.index.IndexStreamSecurityDecorator;
import com.jetbrains.youtrack.db.internal.core.index.IndexMetadata;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransaction;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChangesPerKey;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChangesPerKey.TransactionIndexEntry;
import com.jetbrains.youtrack.db.internal.lucene.OLuceneIndex;
import com.jetbrains.youtrack.db.internal.lucene.engine.LuceneIndexEngine;
import com.jetbrains.youtrack.db.internal.lucene.tx.LuceneTxChanges;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;

public class LuceneIndexNotUnique extends IndexAbstract implements OLuceneIndex {

  public LuceneIndexNotUnique(IndexMetadata im, final Storage storage) {
    super(im, storage);
  }

  @Override
  public long rebuild(DatabaseSessionInternal session, ProgressListener iProgressListener) {
    return super.rebuild(session, iProgressListener);
  }

  @Override
  public boolean remove(DatabaseSessionInternal session, final Object key,
      final Identifiable rid) {

    if (key != null) {
      FrontendTransaction transaction = session.getTransaction();

      transaction.addIndexEntry(
          this, super.getName(), FrontendTransactionIndexChanges.OPERATION.REMOVE, encodeKey(key),
          rid);
      LuceneTxChanges transactionChanges = getTransactionChanges(transaction);
      transactionChanges.remove(session, key, rid);
      return true;
    }
    return true;
  }

  @Override
  public boolean remove(DatabaseSessionInternal session, final Object key) {
    if (key != null) {
      FrontendTransaction transaction = session.getTransaction();
      transaction.addIndexEntry(
          this, super.getName(), FrontendTransactionIndexChanges.OPERATION.REMOVE, encodeKey(key),
          null);
      LuceneTxChanges transactionChanges = getTransactionChanges(transaction);
      transactionChanges.remove(session, key, null);
      return true;
    }
    return true;
  }

  @Override
  public void removeCluster(DatabaseSessionInternal session, String clusterName) {
    acquireExclusiveLock();
    try {
      if (clustersToIndex.remove(clusterName)) {
        session.executeInTx(
            () -> remove(session, "_CLUSTER:" + storage.getClusterIdByName(clusterName)));
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Iterable<TransactionIndexEntry> interpretTxKeyChanges(
      FrontendTransactionIndexChangesPerKey changes) {
    return changes.interpret(FrontendTransactionIndexChangesPerKey.Interpretation.NonUnique);
  }

  @Override
  public void doPut(DatabaseSessionInternal session, AbstractPaginatedStorage storage,
      Object key,
      RID rid) {
    while (true) {
      try {
        storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              try {
                LuceneIndexEngine indexEngine = (LuceneIndexEngine) engine;

                AtomicOperation atomicOperation =
                    storage.getAtomicOperationsManager().getCurrentOperation();
                indexEngine.put(session, atomicOperation, decodeKey(key), rid);
                return null;
              } catch (IOException e) {
                throw BaseException.wrapException(
                    new IndexException("Error during commit of index changes"), e);
              }
            });
        break;
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean doRemove(AbstractPaginatedStorage storage, Object key)
      throws InvalidIndexEngineIdException {
    while (true) {
      try {
        storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              LuceneIndexEngine indexEngine = (LuceneIndexEngine) engine;
              indexEngine.remove(decodeKey(key));
              return true;
            });
        break;
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
    return false;
  }

  @Override
  public boolean doRemove(DatabaseSessionInternal session, AbstractPaginatedStorage storage,
      Object key, RID rid)
      throws InvalidIndexEngineIdException {
    while (true) {
      try {
        storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              LuceneIndexEngine indexEngine = (LuceneIndexEngine) engine;
              indexEngine.remove(decodeKey(key), rid);
              return true;
            });
        break;
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
    return false;
  }

  @Override
  public Object getCollatingValue(Object key) {
    return key;
  }

  public void doDelete(DatabaseSessionInternal session) {
    while (true) {
      try {
        storage.deleteIndexEngine(indexId);
        break;
      } catch (InvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }
    }
  }

  protected Object decodeKey(Object key) {
    return key;
  }

  @Override
  protected void onIndexEngineChange(int indexId) {
    while (true) {
      try {
        storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              LuceneIndexEngine oIndexEngine = (LuceneIndexEngine) engine;
              oIndexEngine.init(im);
              return null;
            });
        break;
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  protected Object encodeKey(Object key) {
    return key;
  }

  private LuceneTxChanges getTransactionChanges(FrontendTransaction transaction) {

    LuceneTxChanges changes = (LuceneTxChanges) transaction.getCustomData(getName());
    if (changes == null) {
      while (true) {
        try {
          changes =
              storage.callIndexEngine(
                  false,
                  indexId,
                  engine -> {
                    LuceneIndexEngine indexEngine = (LuceneIndexEngine) engine;
                    try {
                      return indexEngine.buildTxChanges();
                    } catch (IOException e) {
                      throw BaseException.wrapException(
                          new IndexException("Cannot get searcher from index " + getName()), e);
                    }
                  });
          break;
        } catch (InvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }

      transaction.setCustomData(getName(), changes);
    }
    return changes;
  }

  @Deprecated
  @Override
  public Collection<Identifiable> get(DatabaseSessionInternal session, final Object key) {
    try (Stream<RID> stream = getRids(session, key)) {
      return stream.collect(Collectors.toList());
    }
  }

  @Override
  public Stream<RID> getRidsIgnoreTx(DatabaseSessionInternal session, Object key) {
    while (true) {
      try {
        @SuppressWarnings("unchecked")
        Set<Identifiable> result = (Set<Identifiable>) storage.getIndexValue(session, indexId,
            key);
        return result.stream().map(Identifiable::getIdentity);
        // TODO filter these results based on security
        //          return new HashSet(IndexInternal.securityFilterOnRead(this, result));
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<RID> getRids(DatabaseSessionInternal session, Object key) {
    return session.computeInTx(() -> {
      var transaction = session.getTransaction();
      while (true) {
        try {
          return storage
              .callIndexEngine(
                  false,
                  indexId,
                  engine -> {
                    LuceneIndexEngine indexEngine = (LuceneIndexEngine) engine;
                    return indexEngine.getInTx(session, key, getTransactionChanges(transaction));
                  })
              .stream()
              .map(Identifiable::getIdentity);
        } catch (InvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    });
  }

  @Override
  public LuceneIndexNotUnique put(DatabaseSessionInternal session, final Object key,
      final Identifiable value) {
    final RID rid = value.getIdentity();

    if (!rid.isValid()) {
      if (value instanceof Record) {
        // EARLY SAVE IT
        ((Record) value).save();
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }
    if (key != null) {
      FrontendTransaction transaction = session.getTransaction();
      LuceneTxChanges transactionChanges = getTransactionChanges(transaction);
      transaction.addIndexEntry(
          this, super.getName(), FrontendTransactionIndexChanges.OPERATION.PUT, encodeKey(key),
          value);

      Document luceneDoc;
      while (true) {
        try {
          luceneDoc =
              storage.callIndexEngine(
                  false,
                  indexId,
                  engine -> {
                    LuceneIndexEngine oIndexEngine = (LuceneIndexEngine) engine;
                    return oIndexEngine.buildDocument(session, key, value);
                  });
          break;
        } catch (InvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }

      transactionChanges.put(key, value, luceneDoc);
    }
    return this;
  }

  @Override
  public long size(DatabaseSessionInternal session) {
    return session.computeInTx(() -> {
      while (true) {
        try {
          return storage.callIndexEngine(
              false,
              indexId,
              engine -> {
                FrontendTransaction transaction = session.getTransaction();
                LuceneIndexEngine indexEngine = (LuceneIndexEngine) engine;
                return indexEngine.sizeInTx(getTransactionChanges(transaction));
              });
        } catch (InvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    });
  }

  @Override
  public Stream<RawPair<Object, RID>> streamEntries(DatabaseSessionInternal session,
      Collection<?> keys, boolean ascSortOrder) {

    @SuppressWarnings("resource")
    String query =
        (String)
            keys.stream()
                .findFirst()
                .map(k -> (CompositeKey) k)
                .map(CompositeKey::getKeys)
                .orElse(Collections.singletonList("q=*:*"))
                .get(0);
    return IndexStreamSecurityDecorator.decorateStream(
        this, getRids(session, query).map((rid) -> new RawPair<>(query, rid)));
  }

  @Override
  public Stream<RawPair<Object, RID>> streamEntriesBetween(
      DatabaseSessionInternal session, Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this,
            storage.iterateIndexEntriesBetween(session,
                indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null));
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<RawPair<Object, RID>> streamEntriesMajor(
      DatabaseSessionInternal session, Object fromKey, boolean fromInclusive, boolean ascOrder) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this,
            storage.iterateIndexEntriesMajor(indexId, fromKey, fromInclusive, ascOrder, null));
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<RawPair<Object, RID>> streamEntriesMinor(
      DatabaseSessionInternal session, Object toKey, boolean toInclusive, boolean ascOrder) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this, storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null));
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean isNativeTxSupported() {
    return false;
  }

  @Override
  public Stream<RawPair<Object, RID>> stream(DatabaseSessionInternal session) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this, storage.getIndexStream(indexId, null));
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<RawPair<Object, RID>> descStream(DatabaseSessionInternal session) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this, storage.getIndexStream(indexId, null));
      } catch (InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public IndexSearcher searcher() {
    while (true) {
      try {
        return storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              final LuceneIndexEngine indexEngine = (LuceneIndexEngine) engine;
              return indexEngine.searcher();
            });
      } catch (final InvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return false;
  }
}
