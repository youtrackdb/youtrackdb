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

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.InternalResultSet;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransaction;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges.OPERATION;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Proxied abstract index.
 */
@SuppressWarnings("unchecked")
public abstract class IndexRemote implements Index {

  public static final String QUERY_GET_VALUES_BEETWEN_SELECT = "select from index:`%s` where ";
  public static final String QUERY_GET_VALUES_BEETWEN_INCLUSIVE_FROM_CONDITION = "key >= ?";
  public static final String QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_FROM_CONDITION = "key > ?";
  public static final String QUERY_GET_VALUES_BEETWEN_INCLUSIVE_TO_CONDITION = "key <= ?";
  public static final String QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_TO_CONDITION = "key < ?";
  public static final String QUERY_GET_VALUES_AND_OPERATOR = " and ";
  public static final String QUERY_GET_VALUES_LIMIT = " limit ";
  protected static final String QUERY_ENTRIES = "select key, rid from index:`%s`";
  protected static final String QUERY_ENTRIES_DESC =
      "select key, rid from index:`%s` order by key desc";

  private static final String QUERY_ITERATE_ENTRIES =
      "select from index:`%s` where key in [%s] order by key %s ";
  private static final String QUERY_GET_ENTRIES = "select from index:`%s` where key in [%s]";

  private static final String QUERY_PUT = "insert into index:`%s` (key,rid) values (?,?)";
  private static final String QUERY_REMOVE = "delete from index:`%s` where key = ?";
  private static final String QUERY_REMOVE2 = "delete from index:`%s` where key = ? and rid = ?";
  private static final String QUERY_REMOVE3 = "delete from index:`%s` where rid = ?";
  private static final String QUERY_CONTAINS =
      "select count(*) as size from index:`%s` where key = ?";
  private static final String QUERY_COUNT = "select count(*) as size from index:`%s` where key = ?";
  private static final String QUERY_COUNT_RANGE = "select count(*) as size from index:`%s` where ";
  private static final String QUERY_SIZE = "select count(*) as size from index:`%s`";
  private static final String QUERY_KEY_SIZE = "select indexKeySize('%s') as size";
  private static final String QUERY_KEYS = "select key from index:`%s`";
  private static final String QUERY_REBUILD = "rebuild index %s";
  private static final String QUERY_CLEAR = "delete from index:`%s`";
  private static final String QUERY_DROP = "drop index %s";
  protected final String databaseName;
  private final String wrappedType;
  private final String algorithm;
  private final RID rid;
  protected IndexDefinition indexDefinition;
  protected String name;
  protected EntityImpl configuration;
  protected Set<String> clustersToIndex;

  public IndexRemote(
      final String iName,
      final String iWrappedType,
      final String algorithm,
      final RID iRid,
      final IndexDefinition iIndexDefinition,
      final EntityImpl iConfiguration,
      final Set<String> clustersToIndex,
      String database) {
    this.name = iName;
    this.wrappedType = iWrappedType;
    this.algorithm = algorithm;
    this.rid = iRid;
    this.indexDefinition = iIndexDefinition;
    this.configuration = iConfiguration;
    this.clustersToIndex = new HashSet<String>(clustersToIndex);
    this.databaseName = database;
  }

  public IndexRemote create(
      final IndexMetadata indexMetadata,
      boolean rebuild,
      final ProgressListener progressListener) {
    this.name = indexMetadata.getName();
    return this;
  }

  public IndexRemote delete(DatabaseSessionInternal session) {
    getDatabase().indexQuery(name, String.format(QUERY_DROP, name)).close();
    return this;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public long getRebuildVersion() {
    throw new UnsupportedOperationException();
  }

  public boolean contains(final Object iKey) {
    try (ResultSet result =
        getDatabase().indexQuery(name, String.format(QUERY_CONTAINS, name), iKey)) {
      if (!result.hasNext()) {
        return false;
      }
      return (Long) result.next().getProperty("size") > 0;
    }
  }

  public long count(DatabaseSessionInternal session, final Object iKey) {
    try (ResultSet result =
        getDatabase().indexQuery(name, String.format(QUERY_COUNT, name), iKey)) {
      if (!result.hasNext()) {
        return 0;
      }
      return result.next().getProperty("size");
    }
  }

  public long count(
      final Object iRangeFrom,
      final boolean iFromInclusive,
      final Object iRangeTo,
      final boolean iToInclusive,
      final int maxValuesToFetch) {
    final StringBuilder query = new StringBuilder(QUERY_COUNT_RANGE);

    if (iFromInclusive) {
      query.append(QUERY_GET_VALUES_BEETWEN_INCLUSIVE_FROM_CONDITION);
    } else {
      query.append(QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_FROM_CONDITION);
    }

    query.append(QUERY_GET_VALUES_AND_OPERATOR);

    if (iToInclusive) {
      query.append(QUERY_GET_VALUES_BEETWEN_INCLUSIVE_TO_CONDITION);
    } else {
      query.append(QUERY_GET_VALUES_BEETWEN_EXCLUSIVE_TO_CONDITION);
    }

    if (maxValuesToFetch > 0) {
      query.append(QUERY_GET_VALUES_LIMIT).append(maxValuesToFetch);
    }

    try (ResultSet rs = getDatabase().indexQuery(name, query.toString(), iRangeFrom, iRangeTo)) {
      return rs.next().getProperty("value");
    }
  }

  public IndexRemote put(DatabaseSessionInternal session, final Object key,
      final Identifiable value) {
    final RecordId rid = (RecordId) value.getIdentity();

    if (!rid.isValid()) {
      if (value instanceof Record) {
        // EARLY SAVE IT
        ((Record) value).save();
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }

    DatabaseSessionInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      FrontendTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(this, name, FrontendTransactionIndexChanges.OPERATION.PUT, key, value);
    } else {
      database.begin();
      FrontendTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(this, name, FrontendTransactionIndexChanges.OPERATION.PUT, key, value);
      database.commit();
    }
    return this;
  }

  public boolean remove(DatabaseSessionInternal session, final Object key) {
    DatabaseSessionInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      database.getTransaction().addIndexEntry(this, name, OPERATION.REMOVE, key, null);
    } else {
      database.begin();
      database.getTransaction().addIndexEntry(this, name, OPERATION.REMOVE, key, null);
      database.commit();
    }
    return true;
  }

  public boolean remove(DatabaseSessionInternal session, final Object key,
      final Identifiable rid) {

    DatabaseSessionInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      database.getTransaction().addIndexEntry(this, name, OPERATION.REMOVE, key, rid);
    } else {
      database.begin();
      database.getTransaction().addIndexEntry(this, name, OPERATION.REMOVE, key, rid);
      database.commit();
    }
    return true;
  }

  @Override
  public int getVersion() {
    if (configuration == null) {
      return -1;
    }

    final Integer version = configuration.field(IndexInternal.INDEX_VERSION);
    if (version != null) {
      return version;
    }

    return -1;
  }

  public void automaticRebuild() {
    throw new UnsupportedOperationException("autoRebuild()");
  }

  public long rebuild(DatabaseSessionInternal session) {
    try (ResultSet rs = getDatabase().command(String.format(QUERY_REBUILD, name))) {
      return rs.next().getProperty("totalIndexed");
    }
  }

  public IndexRemote clear(DatabaseSessionInternal session) {
    getDatabase().command(String.format(QUERY_CLEAR, name)).close();
    return this;
  }

  public long getSize(DatabaseSessionInternal session) {
    try (ResultSet result = getDatabase().indexQuery(name, String.format(QUERY_SIZE, name))) {
      if (result.hasNext()) {
        return result.next().getProperty("size");
      }
    }
    return 0;
  }

  public long getKeySize() {
    try (ResultSet result = getDatabase().indexQuery(name, String.format(QUERY_KEY_SIZE, name))) {
      if (result.hasNext()) {
        return result.next().getProperty("size");
      }
    }
    return 0;
  }

  public boolean isAutomatic() {
    return indexDefinition != null && indexDefinition.getClassName() != null;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  public String getName() {
    return name;
  }

  @Override
  public void flush() {
  }

  public String getType() {
    return wrappedType;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public EntityImpl getConfiguration(DatabaseSessionInternal session) {
    return configuration;
  }

  @Override
  public Map<String, ?> getMetadata() {
    var embedded = configuration.<EntityImpl>field("metadata", PropertyType.EMBEDDED);
    var map = embedded.toMap();

    map.remove("@rid");
    map.remove("@class");
    map.remove("@type");
    map.remove("@version");

    return map;
  }

  public RID getIdentity() {
    return rid;
  }

  public IndexInternal getInternal() {
    return null;
  }

  public long rebuild(DatabaseSessionInternal session,
      final ProgressListener iProgressListener) {
    return rebuild(session);
  }

  public PropertyType[] getKeyTypes() {
    if (indexDefinition != null) {
      return indexDefinition.getTypes();
    }
    return new PropertyType[0];
  }

  public Collection<EntityImpl> getEntries(final Collection<?> iKeys) {
    final StringBuilder params = new StringBuilder(128);
    if (!iKeys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < iKeys.size(); i++) {
        params.append(", ?");
      }
    }

    try (ResultSet rs =
        getDatabase()
            .indexQuery(name, String.format(QUERY_GET_ENTRIES, name, params), iKeys.toArray())) {
      return rs.stream().map((res) -> (EntityImpl) res.toEntity()).collect(Collectors.toList());
    }
  }

  public IndexDefinition getDefinition() {
    return indexDefinition;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final IndexRemote that = (IndexRemote) o;

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  public Set<String> getClusters() {
    return Collections.unmodifiableSet(clustersToIndex);
  }

  @Override
  public boolean isRebuilding() {
    return false;
  }

  @Override
  public Object getFirstKey() {
    throw new UnsupportedOperationException("getFirstKey");
  }

  @Override
  public Object getLastKey(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException("getLastKey");
  }

  @Override
  public IndexCursor iterateEntriesBetween(
      DatabaseSessionInternal session, Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public IndexCursor iterateEntriesMajor(DatabaseSessionInternal session, Object fromKey,
      boolean fromInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesMajor");
  }

  @Override
  public IndexCursor iterateEntriesMinor(DatabaseSessionInternal session, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesMinor");
  }

  @Override
  public IndexCursor iterateEntries(DatabaseSessionInternal session, Collection<?> keys,
      boolean ascSortOrder) {

    final StringBuilder params = new StringBuilder(128);
    if (!keys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < keys.size(); i++) {
        params.append(", ?");
      }
    }

    final InternalResultSet copy = new InternalResultSet(); // TODO a raw array instead...?
    try (ResultSet res =
        getDatabase()
            .indexQuery(
                name,
                String.format(QUERY_ITERATE_ENTRIES, name, params, ascSortOrder ? "ASC" : "DESC"),
                keys.toArray())) {

      res.forEachRemaining(x -> copy.add(x));
    }

    return new IndexAbstractCursor() {

      @Override
      public Map.Entry<Object, Identifiable> nextEntry() {
        if (!copy.hasNext()) {
          return null;
        }
        final Result next = copy.next();
        return new Map.Entry<Object, Identifiable>() {
          @Override
          public Object getKey() {
            return next.getProperty("key");
          }

          @Override
          public Identifiable getValue() {
            return next.getProperty("rid");
          }

          @Override
          public Identifiable setValue(Identifiable value) {
            throw new UnsupportedOperationException("cannot set value of index entry");
          }
        };
      }
    };
  }

  @Override
  public IndexCursor cursor(DatabaseSessionInternal session) {
    final InternalResultSet copy = new InternalResultSet(); // TODO a raw array instead...?
    try (ResultSet result = getDatabase().indexQuery(name, String.format(QUERY_ENTRIES, name))) {
      result.forEachRemaining(x -> copy.add(x));
    }

    return new IndexAbstractCursor() {

      @Override
      public Map.Entry<Object, Identifiable> nextEntry() {
        if (!copy.hasNext()) {
          return null;
        }

        final Result value = copy.next();

        return new Map.Entry<Object, Identifiable>() {
          @Override
          public Object getKey() {
            return value.getProperty("key");
          }

          @Override
          public Identifiable getValue() {
            return value.getProperty("rid");
          }

          @Override
          public Identifiable setValue(Identifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };
  }

  @Override
  public IndexCursor descCursor(DatabaseSessionInternal session) {
    final InternalResultSet copy = new InternalResultSet(); // TODO a raw array instead...?
    try (ResultSet result =
        getDatabase().indexQuery(name, String.format(QUERY_ENTRIES_DESC, name))) {
      result.forEachRemaining(x -> copy.add(x));
    }

    return new IndexAbstractCursor() {

      @Override
      public Map.Entry<Object, Identifiable> nextEntry() {
        if (!copy.hasNext()) {
          return null;
        }

        final Result value = copy.next();

        return new Map.Entry<Object, Identifiable>() {
          @Override
          public Object getKey() {
            return value.getProperty("key");
          }

          @Override
          public Identifiable getValue() {
            return value.getProperty("rid");
          }

          @Override
          public Identifiable setValue(Identifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };
  }

  @Override
  public IndexKeyCursor keyCursor() {
    final InternalResultSet copy = new InternalResultSet(); // TODO a raw array instead...?
    try (final ResultSet result =
        getDatabase().indexQuery(name, String.format(QUERY_KEYS, name))) {
      result.forEachRemaining(x -> copy.add(x));
    }
    return new IndexKeyCursor() {

      @Override
      public Object next(int prefetchSize) {
        if (!copy.hasNext()) {
          return null;
        }

        final Result value = copy.next();

        return value.getProperty("key");
      }
    };
  }

  @Override
  public int compareTo(Index index) {
    final String name = index.getName();
    return this.name.compareTo(name);
  }

  protected DatabaseSessionInternal getDatabase() {
    return DatabaseRecordThreadLocal.instance().get();
  }
}
