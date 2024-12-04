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

package com.orientechnologies.lucene.index;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.IndexStreamSecurityDecorator;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.index.OIndexMetadata;
import com.orientechnologies.orient.core.index.YTIndexException;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;

public class OLuceneIndexNotUnique extends OIndexAbstract implements OLuceneIndex {

  public OLuceneIndexNotUnique(OIndexMetadata im, final OStorage storage) {
    super(im, storage);
  }

  @Override
  public long rebuild(YTDatabaseSessionInternal session, OProgressListener iProgressListener) {
    return super.rebuild(session, iProgressListener);
  }

  @Override
  public boolean remove(YTDatabaseSessionInternal session, final Object key,
      final YTIdentifiable rid) {

    if (key != null) {
      OTransaction transaction = session.getTransaction();

      transaction.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.REMOVE, encodeKey(key), rid);
      OLuceneTxChanges transactionChanges = getTransactionChanges(transaction);
      transactionChanges.remove(session, key, rid);
      return true;
    }
    return true;
  }

  @Override
  public boolean remove(YTDatabaseSessionInternal session, final Object key) {
    if (key != null) {
      OTransaction transaction = session.getTransaction();
      transaction.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.REMOVE, encodeKey(key), null);
      OLuceneTxChanges transactionChanges = getTransactionChanges(transaction);
      transactionChanges.remove(session, key, null);
      return true;
    }
    return true;
  }

  @Override
  public void removeCluster(YTDatabaseSessionInternal session, String clusterName) {
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
  public Iterable<OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.NonUnique);
  }

  @Override
  public void doPut(YTDatabaseSessionInternal session, OAbstractPaginatedStorage storage,
      Object key,
      YTRID rid) {
    while (true) {
      try {
        storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              try {
                OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;

                OAtomicOperation atomicOperation =
                    storage.getAtomicOperationsManager().getCurrentOperation();
                indexEngine.put(session, atomicOperation, decodeKey(key), rid);
                return null;
              } catch (IOException e) {
                throw YTException.wrapException(
                    new YTIndexException("Error during commit of index changes"), e);
              }
            });
        break;
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean doRemove(OAbstractPaginatedStorage storage, Object key)
      throws OInvalidIndexEngineIdException {
    while (true) {
      try {
        storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
              indexEngine.remove(decodeKey(key));
              return true;
            });
        break;
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
    return false;
  }

  @Override
  public boolean doRemove(YTDatabaseSessionInternal session, OAbstractPaginatedStorage storage,
      Object key, YTRID rid)
      throws OInvalidIndexEngineIdException {
    while (true) {
      try {
        storage.callIndexEngine(
            false,
            indexId,
            engine -> {
              OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
              indexEngine.remove(decodeKey(key), rid);
              return true;
            });
        break;
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
    return false;
  }

  @Override
  public Object getCollatingValue(Object key) {
    return key;
  }

  public void doDelete(YTDatabaseSessionInternal session) {
    while (true) {
      try {
        storage.deleteIndexEngine(indexId);
        break;
      } catch (OInvalidIndexEngineIdException ignore) {
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
              OLuceneIndexEngine oIndexEngine = (OLuceneIndexEngine) engine;
              oIndexEngine.init(im);
              return null;
            });
        break;
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  protected Object encodeKey(Object key) {
    return key;
  }

  private OLuceneTxChanges getTransactionChanges(OTransaction transaction) {

    OLuceneTxChanges changes = (OLuceneTxChanges) transaction.getCustomData(getName());
    if (changes == null) {
      while (true) {
        try {
          changes =
              storage.callIndexEngine(
                  false,
                  indexId,
                  engine -> {
                    OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
                    try {
                      return indexEngine.buildTxChanges();
                    } catch (IOException e) {
                      throw YTException.wrapException(
                          new YTIndexException("Cannot get searcher from index " + getName()), e);
                    }
                  });
          break;
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }

      transaction.setCustomData(getName(), changes);
    }
    return changes;
  }

  @Deprecated
  @Override
  public Collection<YTIdentifiable> get(YTDatabaseSessionInternal session, final Object key) {
    try (Stream<YTRID> stream = getRids(session, key)) {
      return stream.collect(Collectors.toList());
    }
  }

  @Override
  public Stream<YTRID> getRidsIgnoreTx(YTDatabaseSessionInternal session, Object key) {
    while (true) {
      try {
        @SuppressWarnings("unchecked")
        Set<YTIdentifiable> result = (Set<YTIdentifiable>) storage.getIndexValue(session, indexId,
            key);
        return result.stream().map(YTIdentifiable::getIdentity);
        // TODO filter these results based on security
        //          return new HashSet(OIndexInternal.securityFilterOnRead(this, result));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<YTRID> getRids(YTDatabaseSessionInternal session, Object key) {
    return session.computeInTx(() -> {
      var transaction = session.getTransaction();
      while (true) {
        try {
          return storage
              .callIndexEngine(
                  false,
                  indexId,
                  engine -> {
                    OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
                    return indexEngine.getInTx(session, key, getTransactionChanges(transaction));
                  })
              .stream()
              .map(YTIdentifiable::getIdentity);
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    });
  }

  @Override
  public OLuceneIndexNotUnique put(YTDatabaseSessionInternal session, final Object key,
      final YTIdentifiable value) {
    final YTRID rid = value.getIdentity();

    if (!rid.isValid()) {
      if (value instanceof YTRecord) {
        // EARLY SAVE IT
        ((YTRecord) value).save();
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }
    if (key != null) {
      OTransaction transaction = session.getTransaction();
      OLuceneTxChanges transactionChanges = getTransactionChanges(transaction);
      transaction.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, encodeKey(key), value);

      Document luceneDoc;
      while (true) {
        try {
          luceneDoc =
              storage.callIndexEngine(
                  false,
                  indexId,
                  engine -> {
                    OLuceneIndexEngine oIndexEngine = (OLuceneIndexEngine) engine;
                    return oIndexEngine.buildDocument(session, key, value);
                  });
          break;
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }

      transactionChanges.put(key, value, luceneDoc);
    }
    return this;
  }

  @Override
  public long size(YTDatabaseSessionInternal session) {
    return session.computeInTx(() -> {
      while (true) {
        try {
          return storage.callIndexEngine(
              false,
              indexId,
              engine -> {
                OTransaction transaction = session.getTransaction();
                OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
                return indexEngine.sizeInTx(getTransactionChanges(transaction));
              });
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    });
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> streamEntries(YTDatabaseSessionInternal session,
      Collection<?> keys, boolean ascSortOrder) {

    @SuppressWarnings("resource")
    String query =
        (String)
            keys.stream()
                .findFirst()
                .map(k -> (OCompositeKey) k)
                .map(OCompositeKey::getKeys)
                .orElse(Collections.singletonList("q=*:*"))
                .get(0);
    return IndexStreamSecurityDecorator.decorateStream(
        this, getRids(session, query).map((rid) -> new ORawPair<>(query, rid)));
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> streamEntriesBetween(
      YTDatabaseSessionInternal session, Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this,
            storage.iterateIndexEntriesBetween(session,
                indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> streamEntriesMajor(
      YTDatabaseSessionInternal session, Object fromKey, boolean fromInclusive, boolean ascOrder) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this,
            storage.iterateIndexEntriesMajor(indexId, fromKey, fromInclusive, ascOrder, null));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> streamEntriesMinor(
      YTDatabaseSessionInternal session, Object toKey, boolean toInclusive, boolean ascOrder) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this, storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean isNativeTxSupported() {
    return false;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> stream(YTDatabaseSessionInternal session) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this, storage.getIndexStream(indexId, null));
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> descStream(YTDatabaseSessionInternal session) {
    while (true) {
      try {
        return IndexStreamSecurityDecorator.decorateStream(
            this, storage.getIndexStream(indexId, null));
      } catch (OInvalidIndexEngineIdException e) {
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
              final OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
              return indexEngine.searcher();
            });
      } catch (final OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return false;
  }
}
