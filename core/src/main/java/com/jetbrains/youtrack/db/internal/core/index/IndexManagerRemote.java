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

import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.MultiKey;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.MetadataUpdateListener;
import com.jetbrains.youtrack.db.internal.core.dictionary.Dictionary;
import com.jetbrains.youtrack.db.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.Metadata;
import com.jetbrains.youtrack.db.internal.core.metadata.MetadataDefault;
import com.jetbrains.youtrack.db.internal.core.metadata.MetadataInternal;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sharding.auto.AutoShardingIndexFactory;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLCreateIndex;
import com.jetbrains.youtrack.db.internal.core.storage.StorageInfo;
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

public class IndexManagerRemote implements IndexManagerAbstract {

  private final AtomicBoolean skipPush = new AtomicBoolean(false);
  private static final String QUERY_DROP = "drop index `%s` if exists";
  private final StorageInfo storage;
  // values of this Map should be IMMUTABLE !! for thread safety reasons.
  protected final Map<String, Map<MultiKey, Set<Index>>> classPropertyIndex =
      new ConcurrentHashMap<>();
  protected Map<String, Index> indexes = new ConcurrentHashMap<>();
  protected String defaultClusterName = MetadataDefault.CLUSTER_INDEX_NAME;
  protected final AtomicInteger writeLockNesting = new AtomicInteger();
  protected final ReadWriteLock lock = new ReentrantReadWriteLock();

  public IndexManagerRemote(StorageInfo storage) {
    super();
    this.storage = storage;
  }

  public void load(DatabaseSessionInternal database) {
    if (!autoRecreateIndexesAfterCrash(database)) {
      acquireExclusiveLock();
      try {
        if (database.getStorageInfo().getConfiguration().getIndexMgrRecordId() == null)
        // @COMPATIBILITY: CREATE THE INDEX MGR
        {
          create(database);
        }

        // RELOAD IT
        RecordId id =
            new RecordId(database.getStorageInfo().getConfiguration().getIndexMgrRecordId());

        database.executeInTx(
            () -> {
              EntityImpl entity = database.load(id);
              fromStream(entity);
            });
      } finally {
        releaseExclusiveLock();
      }
    }
  }

  public void reload(DatabaseSessionInternal session) {
    acquireExclusiveLock();
    try {
      DatabaseSessionInternal database = getDatabase();
      RecordId id =
          new RecordId(database.getStorageInfo().getConfiguration().getIndexMgrRecordId());

      database.executeInTx(
          () -> {
            EntityImpl entity = database.load(id);
            fromStream(entity);
          });
    } finally {
      releaseExclusiveLock();
    }
  }

  public void save(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException();
  }

  public void addClusterToIndex(DatabaseSessionInternal session, final String clusterName,
      final String indexName) {
    throw new UnsupportedOperationException();
  }

  public void removeClusterFromIndex(DatabaseSessionInternal session, final String clusterName,
      final String indexName) {
    throw new UnsupportedOperationException();
  }

  public void create(DatabaseSessionInternal database) {
    throw new UnsupportedOperationException();
  }

  public Collection<? extends Index> getIndexes(DatabaseSessionInternal database) {
    final Collection<Index> rawResult = indexes.values();
    final List<Index> result = new ArrayList<>(rawResult.size());
    for (final Index index : rawResult) {
      result.add(index);
    }
    return result;
  }

  public Index getRawIndex(final String iName) {
    final Index index = indexes.get(iName);
    return index;
  }

  public Index getIndex(DatabaseSessionInternal database, final String iName) {
    final Index index = indexes.get(iName);
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
      DatabaseSessionInternal database, final String defaultClusterName) {
    acquireExclusiveLock();
    try {
      this.defaultClusterName = defaultClusterName;
    } finally {
      releaseExclusiveLock();
    }
  }

  public Dictionary<Record> getDictionary(DatabaseSessionInternal database) {
    Index idx;
    acquireSharedLock();
    try {
      idx = getIndex(database, DICTIONARY_NAME);
    } finally {
      releaseSharedLock();
    }
    assert idx != null;
    return new Dictionary<>(idx);
  }

  public EntityImpl getConfiguration(DatabaseSessionInternal session) {
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

  public Set<Index> getClassInvolvedIndexes(
      DatabaseSessionInternal database, final String className, Collection<String> fields) {
    final MultiKey multiKey = new MultiKey(fields);

    final Map<MultiKey, Set<Index>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null || !propertyIndex.containsKey(multiKey)) {
      return Collections.emptySet();
    }

    final Set<Index> rawResult = propertyIndex.get(multiKey);
    final Set<Index> transactionalResult = new HashSet<>(rawResult.size());
    for (final Index index : rawResult) {
      // ignore indexes that ignore null values on partial match
      if (fields.size() == index.getDefinition().getFields().size()
          || !index.getDefinition().isNullValuesIgnored()) {
        transactionalResult.add(index);
      }
    }

    return transactionalResult;
  }

  public Set<Index> getClassInvolvedIndexes(
      DatabaseSessionInternal database, final String className, final String... fields) {
    return getClassInvolvedIndexes(database, className, Arrays.asList(fields));
  }

  public boolean areIndexed(final String className, Collection<String> fields) {
    final MultiKey multiKey = new MultiKey(fields);

    final Map<MultiKey, Set<Index>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null) {
      return false;
    }

    return propertyIndex.containsKey(multiKey) && !propertyIndex.get(multiKey).isEmpty();
  }

  public boolean areIndexed(final String className, final String... fields) {
    return areIndexed(className, Arrays.asList(fields));
  }

  public Set<Index> getClassIndexes(DatabaseSessionInternal database, final String className) {
    final HashSet<Index> coll = new HashSet<Index>(4);
    getClassIndexes(database, className, coll);
    return coll;
  }

  public void getClassIndexes(
      DatabaseSessionInternal database, final String className,
      final Collection<Index> indexes) {
    getClassRawIndexes(className, indexes);
  }

  public void getClassRawIndexes(final String className, final Collection<Index> indexes) {
    final Map<MultiKey, Set<Index>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex == null) {
      return;
    }

    for (final Set<Index> propertyIndexes : propertyIndex.values()) {
      indexes.addAll(propertyIndexes);
    }
  }

  public IndexUnique getClassUniqueIndex(final String className) {
    final Map<MultiKey, Set<Index>> propertyIndex = getIndexOnProperty(className);

    if (propertyIndex != null) {
      for (final Set<Index> propertyIndexes : propertyIndex.values()) {
        for (final Index index : propertyIndexes) {
          if (index instanceof IndexUnique) {
            return (IndexUnique) index;
          }
        }
      }
    }

    return null;
  }

  public Index getClassIndex(
      DatabaseSessionInternal database, String className, String indexName) {
    className = className.toLowerCase();

    final Index index = indexes.get(indexName);
    if (index != null
        && index.getDefinition() != null
        && index.getDefinition().getClassName() != null
        && className.equals(index.getDefinition().getClassName().toLowerCase())) {
      return index;
    }
    return null;
  }

  public Index getClassAutoShardingIndex(DatabaseSessionInternal database, String className) {
    className = className.toLowerCase();

    // LOOK FOR INDEX
    for (Index index : indexes.values()) {
      if (index != null
          && AutoShardingIndexFactory.AUTOSHARDING_ALGORITHM.equals(index.getAlgorithm())
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
    final DatabaseSessionInternal databaseRecord = getDatabaseIfDefined();
    if (databaseRecord != null && !databaseRecord.isClosed()) {
      final MetadataInternal metadata = databaseRecord.getMetadata();
      if (metadata != null) {
        metadata.makeThreadLocalSchemaSnapshot();
      }
    }
    lock.writeLock().lock();
  }

  void internalReleaseExclusiveLock() {
    lock.writeLock().unlock();

    final DatabaseSessionInternal databaseRecord = getDatabaseIfDefined();
    if (databaseRecord != null && !databaseRecord.isClosed()) {
      final Metadata metadata = databaseRecord.getMetadata();
      if (metadata != null) {
        ((MetadataInternal) metadata).clearThreadLocalSchemaSnapshot();
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

  protected static DatabaseSessionInternal getDatabase() {
    return DatabaseRecordThreadLocal.instance().get();
  }

  private static DatabaseSessionInternal getDatabaseIfDefined() {
    return DatabaseRecordThreadLocal.instance().getIfDefined();
  }

  void addIndexInternal(final Index index) {
    acquireExclusiveLock();
    try {
      indexes.put(index.getName(), index);

      final IndexDefinition indexDefinition = index.getDefinition();
      if (indexDefinition == null || indexDefinition.getClassName() == null) {
        return;
      }

      Map<MultiKey, Set<Index>> propertyIndex =
          getIndexOnProperty(indexDefinition.getClassName());

      if (propertyIndex == null) {
        propertyIndex = new HashMap<>();
      } else {
        propertyIndex = new HashMap<>(propertyIndex);
      }

      final int paramCount = indexDefinition.getParamCount();

      for (int i = 1; i <= paramCount; i++) {
        final List<String> fields = indexDefinition.getFields().subList(0, i);
        final MultiKey multiKey = new MultiKey(fields);
        Set<Index> indexSet = propertyIndex.get(multiKey);

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

  static Map<MultiKey, Set<Index>> copyPropertyMap(Map<MultiKey, Set<Index>> original) {
    final Map<MultiKey, Set<Index>> result = new HashMap<>();

    for (Map.Entry<MultiKey, Set<Index>> entry : original.entrySet()) {
      Set<Index> indexes = new HashSet<>(entry.getValue());
      assert indexes.equals(entry.getValue());

      result.put(entry.getKey(), Collections.unmodifiableSet(indexes));
    }

    assert result.equals(original);

    return Collections.unmodifiableMap(result);
  }

  private Map<MultiKey, Set<Index>> getIndexOnProperty(final String className) {
    acquireSharedLock();
    try {

      return classPropertyIndex.get(className.toLowerCase());

    } finally {
      releaseSharedLock();
    }
  }

  public Index createIndex(
      DatabaseSessionInternal database,
      final String iName,
      final String iType,
      final IndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex,
      final ProgressListener progressListener,
      EntityImpl metadata,
      String engine) {

    String createIndexDDL;
    if (iIndexDefinition != null) {
      createIndexDDL = iIndexDefinition.toCreateIndexDDL(iName, iType, engine);
    } else {
      createIndexDDL = new SimpleKeyIndexDefinition().toCreateIndexDDL(iName, iType, engine);
    }

    if (metadata != null) {
      createIndexDDL +=
          " " + CommandExecutorSQLCreateIndex.KEYWORD_METADATA + " " + metadata.toJSON();
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
    } catch (CommandExecutionException x) {
      throw new IndexException(x.getMessage());
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public Index createIndex(
      DatabaseSessionInternal database,
      String iName,
      String iType,
      IndexDefinition indexDefinition,
      int[] clusterIdsToIndex,
      ProgressListener progressListener,
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

  public void dropIndex(DatabaseSessionInternal database, final String iIndexName) {
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

  public EntityImpl toStream(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException("Remote index cannot be streamed");
  }

  public void recreateIndexes() {
    throw new UnsupportedOperationException("recreateIndexes()");
  }

  @Override
  public void recreateIndexes(DatabaseSessionInternal database) {
    throw new UnsupportedOperationException("recreateIndexes(DatabaseSessionInternal)");
  }

  public void waitTillIndexRestore() {
  }

  @Override
  public boolean autoRecreateIndexesAfterCrash(DatabaseSessionInternal database) {
    return false;
  }

  public boolean autoRecreateIndexesAfterCrash() {
    return false;
  }

  public void removeClassPropertyIndex(DatabaseSessionInternal session, Index idx) {
  }

  protected Index getRemoteIndexInstance(
      boolean isMultiValueIndex,
      String type,
      String name,
      String algorithm,
      Set<String> clustersToIndex,
      IndexDefinition indexDefinition,
      RID identity,
      EntityImpl configuration) {
    if (isMultiValueIndex) {
      return new IndexRemoteMultiValue(
          name,
          type,
          algorithm,
          identity,
          indexDefinition,
          configuration,
          clustersToIndex,
          storage.getName());
    }

    return new IndexRemoteOneValue(
        name,
        type,
        algorithm,
        identity,
        indexDefinition,
        configuration,
        clustersToIndex,
        storage.getName());
  }

  protected void fromStream(EntityImpl entity) {
    acquireExclusiveLock();
    try {
      clearMetadata();

      final Collection<EntityImpl> idxs = entity.field(CONFIG_INDEXES);
      if (idxs != null) {
        for (EntityImpl d : idxs) {
          d.setLazyLoad(false);
          try {
            final boolean isMultiValue =
                DefaultIndexFactory.isMultiValueIndex(d.field(IndexInternal.CONFIG_TYPE));

            final IndexMetadata newIndexMetadata = IndexAbstract.loadMetadataFromDoc(d);

            addIndexInternal(
                getRemoteIndexInstance(
                    isMultiValue,
                    newIndexMetadata.getType(),
                    newIndexMetadata.getName(),
                    newIndexMetadata.getAlgorithm(),
                    newIndexMetadata.getClustersToIndex(),
                    newIndexMetadata.getIndexDefinition(),
                    d.field(IndexAbstract.CONFIG_MAP_RID),
                    d));
          } catch (Exception e) {
            LogManager.instance()
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
    DatabaseSessionInternal database = getDatabaseIfDefined();
    if (database != null) {
      database
          .getSharedContext()
          .getSchema()
          .forceSnapshot(DatabaseRecordThreadLocal.instance().get());
    }
    internalReleaseExclusiveLock();
    if (val == 0 && database != null) {
      for (MetadataUpdateListener listener : database.getSharedContext().browseListeners()) {
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

  protected StorageInfo getStorage() {
    return storage;
  }

  public EntityImpl getDocument(DatabaseSessionInternal session) {
    return null;
  }

  public Index preProcessBeforeReturn(DatabaseSessionInternal database, Index index) {
    return index;
  }
}
