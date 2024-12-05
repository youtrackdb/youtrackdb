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

import com.jetbrains.youtrack.db.internal.common.listener.OProgressListener;
import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.common.util.OMultiKey;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.OMetadataUpdateListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.dictionary.ODictionary;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadata;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadataDefault;
import com.jetbrains.youtrack.db.internal.core.metadata.OMetadataInternal;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sharding.auto.OAutoShardingIndexFactory;
import com.jetbrains.youtrack.db.internal.core.sql.OCommandExecutorSQLCreateIndex;
import com.jetbrains.youtrack.db.internal.core.storage.OStorageInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OIndexManagerRemote implements OIndexManagerAbstract {

  private final AtomicBoolean skipPush = new AtomicBoolean(false);
  private static final String QUERY_DROP = "drop index `%s` if exists";
  private final OStorageInfo storage;
  // values of this Map should be IMMUTABLE !! for thread safety reasons.
  protected final Map<String, Map<OMultiKey, Set<OIndex>>> classPropertyIndex =
      new ConcurrentHashMap<>();
  protected Map<String, OIndex> indexes = new ConcurrentHashMap<>();
  protected String defaultClusterName = OMetadataDefault.CLUSTER_INDEX_NAME;
  protected final AtomicInteger writeLockNesting = new AtomicInteger();
  protected final ReadWriteLock lock = new ReentrantReadWriteLock();

  public OIndexManagerRemote(OStorageInfo storage) {
    super();
    this.storage = storage;
  }

  public void load(YTDatabaseSessionInternal database) {
    if (!autoRecreateIndexesAfterCrash(database)) {
      acquireExclusiveLock();
      try {
        if (database.getStorageInfo().getConfiguration().getIndexMgrRecordId() == null)
        // @COMPATIBILITY: CREATE THE INDEX MGR
        {
          create(database);
        }

        // RELOAD IT
        YTRecordId id =
            new YTRecordId(database.getStorageInfo().getConfiguration().getIndexMgrRecordId());

        database.executeInTx(
            () -> {
              EntityImpl document = database.load(id);
              fromStream(document);
            });
      } finally {
        releaseExclusiveLock();
      }
    }
  }

  public void reload(YTDatabaseSessionInternal session) {
    acquireExclusiveLock();
    try {
      YTDatabaseSessionInternal database = getDatabase();
      YTRecordId id =
          new YTRecordId(database.getStorageInfo().getConfiguration().getIndexMgrRecordId());

      database.executeInTx(
          () -> {
            EntityImpl document = database.load(id);
            fromStream(document);
          });
    } finally {
      releaseExclusiveLock();
    }
  }

  public void save(YTDatabaseSessionInternal session) {
    throw new UnsupportedOperationException();
  }

  public void addClusterToIndex(YTDatabaseSessionInternal session, final String clusterName,
      final String indexName) {
    throw new UnsupportedOperationException();
  }

  public void removeClusterFromIndex(YTDatabaseSessionInternal session, final String clusterName,
      final String indexName) {
    throw new UnsupportedOperationException();
  }

  public void create(YTDatabaseSessionInternal database) {
    throw new UnsupportedOperationException();
  }

  public Collection<? extends OIndex> getIndexes(YTDatabaseSessionInternal database) {
    final Collection<OIndex> rawResult = indexes.values();
    final List<OIndex> result = new ArrayList<>(rawResult.size());
    for (final OIndex index : rawResult) {
      result.add(index);
    }
    return result;
  }

  public OIndex getRawIndex(final String iName) {
    final OIndex index = indexes.get(iName);
    return index;
  }

  public OIndex getIndex(YTDatabaseSessionInternal database, final String iName) {
    final OIndex index = indexes.get(iName);
    return index;
  }

  public boolean existsIndex(final String iName) {
    return indexes.containsKey(iName);
  }

  public String getDefaultClusterName() {
    acquireSharedLock();
    try {
      return defaultClusterName;
    } finally {
      releaseSharedLock();
    }
  }

  public void setDefaultClusterName(
      YTDatabaseSessionInternal database, final String defaultClusterName) {
    acquireExclusiveLock();
    try {
      this.defaultClusterName = defaultClusterName;
    } finally {
      releaseExclusiveLock();
    }
  }

  public ODictionary<Record> getDictionary(YTDatabaseSessionInternal database) {
    OIndex idx;
    acquireSharedLock();
    try {
      idx = getIndex(database, DICTIONARY_NAME);
    } finally {
      releaseSharedLock();
    }
    assert idx != null;
    return new ODictionary<>(idx);
  }

  public EntityImpl getConfiguration(YTDatabaseSessionInternal session) {
    acquireSharedLock();

    try {
      return getDocument(session);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void close() {
    indexes.clear();
    classPropertyIndex.clear();
  }

  public Set<OIndex> getClassInvolvedIndexes(
      YTDatabaseSessionInternal database, final String className, Collection<String> fields) {
    final OMultiKey multiKey = new OMultiKey(fields);

    final Map<OMultiKey, Set<OIndex>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null || !propertyIndex.containsKey(multiKey)) {
      return Collections.emptySet();
    }

    final Set<OIndex> rawResult = propertyIndex.get(multiKey);
    final Set<OIndex> transactionalResult = new HashSet<>(rawResult.size());
    for (final OIndex index : rawResult) {
      // ignore indexes that ignore null values on partial match
      if (fields.size() == index.getDefinition().getFields().size()
          || !index.getDefinition().isNullValuesIgnored()) {
        transactionalResult.add(index);
      }
    }

    return transactionalResult;
  }

  public Set<OIndex> getClassInvolvedIndexes(
      YTDatabaseSessionInternal database, final String className, final String... fields) {
    return getClassInvolvedIndexes(database, className, Arrays.asList(fields));
  }

  public boolean areIndexed(final String className, Collection<String> fields) {
    final OMultiKey multiKey = new OMultiKey(fields);

    final Map<OMultiKey, Set<OIndex>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null) {
      return false;
    }

    return propertyIndex.containsKey(multiKey) && !propertyIndex.get(multiKey).isEmpty();
  }

  public boolean areIndexed(final String className, final String... fields) {
    return areIndexed(className, Arrays.asList(fields));
  }

  public Set<OIndex> getClassIndexes(YTDatabaseSessionInternal database, final String className) {
    final HashSet<OIndex> coll = new HashSet<OIndex>(4);
    getClassIndexes(database, className, coll);
    return coll;
  }

  public void getClassIndexes(
      YTDatabaseSessionInternal database, final String className,
      final Collection<OIndex> indexes) {
    getClassRawIndexes(className, indexes);
  }

  public void getClassRawIndexes(final String className, final Collection<OIndex> indexes) {
    final Map<OMultiKey, Set<OIndex>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null) {
      return;
    }

    for (final Set<OIndex> propertyIndexes : propertyIndex.values()) {
      indexes.addAll(propertyIndexes);
    }
  }

  public OIndexUnique getClassUniqueIndex(final String className) {
    final Map<OMultiKey, Set<OIndex>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex != null) {
      for (final Set<OIndex> propertyIndexes : propertyIndex.values()) {
        for (final OIndex index : propertyIndexes) {
          if (index instanceof OIndexUnique) {
            return (OIndexUnique) index;
          }
        }
      }
    }

    return null;
  }

  public OIndex getClassIndex(
      YTDatabaseSessionInternal database, String className, String indexName) {
    className = className.toLowerCase();

    final OIndex index = indexes.get(indexName);
    if (index != null
        && index.getDefinition() != null
        && index.getDefinition().getClassName() != null
        && className.equals(index.getDefinition().getClassName().toLowerCase())) {
      return index;
    }
    return null;
  }

  public OIndex getClassAutoShardingIndex(YTDatabaseSessionInternal database, String className) {
    className = className.toLowerCase();

    // LOOK FOR INDEX
    for (OIndex index : indexes.values()) {
      if (index != null
          && OAutoShardingIndexFactory.AUTOSHARDING_ALGORITHM.equals(index.getAlgorithm())
          && index.getDefinition() != null
          && index.getDefinition().getClassName() != null
          && className.equals(index.getDefinition().getClassName().toLowerCase())) {
        return index;
      }
    }
    return null;
  }

  private void acquireSharedLock() {
    lock.readLock().lock();
  }

  private void releaseSharedLock() {
    lock.readLock().unlock();
  }

  void internalAcquireExclusiveLock() {
    final YTDatabaseSessionInternal databaseRecord = getDatabaseIfDefined();
    if (databaseRecord != null && !databaseRecord.isClosed()) {
      final OMetadataInternal metadata = databaseRecord.getMetadata();
      if (metadata != null) {
        metadata.makeThreadLocalSchemaSnapshot();
      }
    }
    lock.writeLock().lock();
  }

  void internalReleaseExclusiveLock() {
    lock.writeLock().unlock();

    final YTDatabaseSessionInternal databaseRecord = getDatabaseIfDefined();
    if (databaseRecord != null && !databaseRecord.isClosed()) {
      final OMetadata metadata = databaseRecord.getMetadata();
      if (metadata != null) {
        ((OMetadataInternal) metadata).clearThreadLocalSchemaSnapshot();
      }
    }
  }

  void clearMetadata() {
    acquireExclusiveLock();
    try {
      indexes.clear();
      classPropertyIndex.clear();
    } finally {
      releaseExclusiveLock();
    }
  }

  protected static YTDatabaseSessionInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  private static YTDatabaseSessionInternal getDatabaseIfDefined() {
    return ODatabaseRecordThreadLocal.instance().getIfDefined();
  }

  void addIndexInternal(final OIndex index) {
    acquireExclusiveLock();
    try {
      indexes.put(index.getName(), index);

      final OIndexDefinition indexDefinition = index.getDefinition();
      if (indexDefinition == null || indexDefinition.getClassName() == null) {
        return;
      }

      Map<OMultiKey, Set<OIndex>> propertyIndex =
          getIndexOnProperty(indexDefinition.getClassName());

      if (propertyIndex == null) {
        propertyIndex = new HashMap<>();
      } else {
        propertyIndex = new HashMap<>(propertyIndex);
      }

      final int paramCount = indexDefinition.getParamCount();

      for (int i = 1; i <= paramCount; i++) {
        final List<String> fields = indexDefinition.getFields().subList(0, i);
        final OMultiKey multiKey = new OMultiKey(fields);
        Set<OIndex> indexSet = propertyIndex.get(multiKey);

        if (indexSet == null) {
          indexSet = new HashSet<>();
        } else {
          indexSet = new HashSet<>(indexSet);
        }

        indexSet.add(index);
        propertyIndex.put(multiKey, indexSet);
      }

      classPropertyIndex.put(
          indexDefinition.getClassName().toLowerCase(), copyPropertyMap(propertyIndex));
    } finally {
      releaseExclusiveLock();
    }
  }

  static Map<OMultiKey, Set<OIndex>> copyPropertyMap(Map<OMultiKey, Set<OIndex>> original) {
    final Map<OMultiKey, Set<OIndex>> result = new HashMap<>();

    for (Map.Entry<OMultiKey, Set<OIndex>> entry : original.entrySet()) {
      Set<OIndex> indexes = new HashSet<>(entry.getValue());
      assert indexes.equals(entry.getValue());

      result.put(entry.getKey(), Collections.unmodifiableSet(indexes));
    }

    assert result.equals(original);

    return Collections.unmodifiableMap(result);
  }

  private Map<OMultiKey, Set<OIndex>> getIndexOnProperty(final String className) {
    acquireSharedLock();
    try {

      return classPropertyIndex.get(className.toLowerCase());

    } finally {
      releaseSharedLock();
    }
  }

  public OIndex createIndex(
      YTDatabaseSessionInternal database,
      final String iName,
      final String iType,
      final OIndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex,
      final OProgressListener progressListener,
      EntityImpl metadata,
      String engine) {

    String createIndexDDL;
    if (iIndexDefinition != null) {
      createIndexDDL = iIndexDefinition.toCreateIndexDDL(iName, iType, engine);
    } else {
      createIndexDDL = new OSimpleKeyIndexDefinition().toCreateIndexDDL(iName, iType, engine);
    }

    if (metadata != null) {
      createIndexDDL +=
          " " + OCommandExecutorSQLCreateIndex.KEYWORD_METADATA + " " + metadata.toJSON();
    }

    acquireExclusiveLock();
    try {
      if (progressListener != null) {
        progressListener.onBegin(this, 0, false);
      }

      database.command(createIndexDDL).close();

      if (progressListener != null) {
        progressListener.onCompletition(database, this, true);
      }

      reload(database);

      return indexes.get(iName);
    } catch (YTCommandExecutionException x) {
      throw new YTIndexException(x.getMessage());
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OIndex createIndex(
      YTDatabaseSessionInternal database,
      String iName,
      String iType,
      OIndexDefinition indexDefinition,
      int[] clusterIdsToIndex,
      OProgressListener progressListener,
      EntityImpl metadata) {
    return createIndex(
        database,
        iName,
        iType,
        indexDefinition,
        clusterIdsToIndex,
        progressListener,
        metadata,
        null);
  }

  public void dropIndex(YTDatabaseSessionInternal database, final String iIndexName) {
    acquireExclusiveLock();
    try {
      final String text = String.format(QUERY_DROP, iIndexName);
      database.command(text).close();

      // REMOVE THE INDEX LOCALLY
      indexes.remove(iIndexName);
      reload(database);

    } finally {
      releaseExclusiveLock();
    }
  }

  public EntityImpl toStream(YTDatabaseSessionInternal session) {
    throw new UnsupportedOperationException("Remote index cannot be streamed");
  }

  public void recreateIndexes() {
    throw new UnsupportedOperationException("recreateIndexes()");
  }

  @Override
  public void recreateIndexes(YTDatabaseSessionInternal database) {
    throw new UnsupportedOperationException("recreateIndexes(YTDatabaseSessionInternal)");
  }

  public void waitTillIndexRestore() {
  }

  @Override
  public boolean autoRecreateIndexesAfterCrash(YTDatabaseSessionInternal database) {
    return false;
  }

  public boolean autoRecreateIndexesAfterCrash() {
    return false;
  }

  public void removeClassPropertyIndex(YTDatabaseSessionInternal session, OIndex idx) {
  }

  protected OIndex getRemoteIndexInstance(
      boolean isMultiValueIndex,
      String type,
      String name,
      String algorithm,
      Set<String> clustersToIndex,
      OIndexDefinition indexDefinition,
      YTRID identity,
      EntityImpl configuration) {
    if (isMultiValueIndex) {
      return new OIndexRemoteMultiValue(
          name,
          type,
          algorithm,
          identity,
          indexDefinition,
          configuration,
          clustersToIndex,
          storage.getName());
    }

    return new OIndexRemoteOneValue(
        name,
        type,
        algorithm,
        identity,
        indexDefinition,
        configuration,
        clustersToIndex,
        storage.getName());
  }

  protected void fromStream(EntityImpl document) {
    acquireExclusiveLock();
    try {
      clearMetadata();

      final Collection<EntityImpl> idxs = document.field(CONFIG_INDEXES);
      if (idxs != null) {
        for (EntityImpl d : idxs) {
          d.setLazyLoad(false);
          try {
            final boolean isMultiValue =
                ODefaultIndexFactory.isMultiValueIndex(d.field(OIndexInternal.CONFIG_TYPE));

            final OIndexMetadata newIndexMetadata = OIndexAbstract.loadMetadataFromDoc(d);

            addIndexInternal(
                getRemoteIndexInstance(
                    isMultiValue,
                    newIndexMetadata.getType(),
                    newIndexMetadata.getName(),
                    newIndexMetadata.getAlgorithm(),
                    newIndexMetadata.getClustersToIndex(),
                    newIndexMetadata.getIndexDefinition(),
                    d.field(OIndexAbstract.CONFIG_MAP_RID),
                    d));
          } catch (Exception e) {
            OLogManager.instance()
                .error(this, "Error on loading of index by configuration: %s", e, d);
          }
        }
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  protected void acquireExclusiveLock() {
    skipPush.set(true);
  }

  protected void releaseExclusiveLock() {
    skipPush.set(false);
  }

  protected void realAcquireExclusiveLock() {
    internalAcquireExclusiveLock();
    writeLockNesting.incrementAndGet();
  }

  protected void realReleaseExclusiveLock() {
    int val = writeLockNesting.decrementAndGet();
    YTDatabaseSessionInternal database = getDatabaseIfDefined();
    if (database != null) {
      database
          .getSharedContext()
          .getSchema()
          .forceSnapshot(ODatabaseRecordThreadLocal.instance().get());
    }
    internalReleaseExclusiveLock();
    if (val == 0 && database != null) {
      for (OMetadataUpdateListener listener : database.getSharedContext().browseListeners()) {
        listener.onIndexManagerUpdate(database, database.getName(), this);
      }
    }
  }

  public void update(EntityImpl indexManager) {
    if (!skipPush.get()) {
      realAcquireExclusiveLock();
      try {
        fromStream(indexManager);
      } finally {
        realReleaseExclusiveLock();
      }
    }
  }

  protected OStorageInfo getStorage() {
    return storage;
  }

  public EntityImpl getDocument(YTDatabaseSessionInternal session) {
    return null;
  }

  public OIndex preProcessBeforeReturn(YTDatabaseSessionInternal database, OIndex index) {
    return index;
  }
}
