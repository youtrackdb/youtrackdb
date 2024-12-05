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
package com.orientechnologies.core.index;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTInternalResultSet;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.core.tx.OTransaction;
import com.orientechnologies.core.tx.OTransactionIndexChanges;
import com.orientechnologies.core.tx.OTransactionIndexChanges.OPERATION;
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
public abstract class OIndexRemote implements OIndex {

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
  private final YTRID rid;
  protected OIndexDefinition indexDefinition;
  protected String name;
  protected YTEntityImpl configuration;
  protected Set<String> clustersToIndex;

  public OIndexRemote(
      final String iName,
      final String iWrappedType,
      final String algorithm,
      final YTRID iRid,
      final OIndexDefinition iIndexDefinition,
      final YTEntityImpl iConfiguration,
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

  public OIndexRemote create(
      final OIndexMetadata indexMetadata,
      boolean rebuild,
      final OProgressListener progressListener) {
    this.name = indexMetadata.getName();
    return this;
  }

  public OIndexRemote delete(YTDatabaseSessionInternal session) {
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
    try (YTResultSet result =
        getDatabase().indexQuery(name, String.format(QUERY_CONTAINS, name), iKey)) {
      if (!result.hasNext()) {
        return false;
      }
      return (Long) result.next().getProperty("size") > 0;
    }
  }

  public long count(YTDatabaseSessionInternal session, final Object iKey) {
    try (YTResultSet result =
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

    try (YTResultSet rs = getDatabase().indexQuery(name, query.toString(), iRangeFrom, iRangeTo)) {
      return rs.next().getProperty("value");
    }
  }

  public OIndexRemote put(YTDatabaseSessionInternal session, final Object key,
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

    YTDatabaseSessionInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      OTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(this, name, OTransactionIndexChanges.OPERATION.PUT, key, value);
    } else {
      database.begin();
      OTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(this, name, OTransactionIndexChanges.OPERATION.PUT, key, value);
      database.commit();
    }
    return this;
  }

  public boolean remove(YTDatabaseSessionInternal session, final Object key) {
    YTDatabaseSessionInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      database.getTransaction().addIndexEntry(this, name, OPERATION.REMOVE, key, null);
    } else {
      database.begin();
      database.getTransaction().addIndexEntry(this, name, OPERATION.REMOVE, key, null);
      database.commit();
    }
    return true;
  }

  public boolean remove(YTDatabaseSessionInternal session, final Object key,
      final YTIdentifiable rid) {

    YTDatabaseSessionInternal database = getDatabase();
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

    final Integer version = configuration.field(OIndexInternal.INDEX_VERSION);
    if (version != null) {
      return version;
    }

    return -1;
  }

  public void automaticRebuild() {
    throw new UnsupportedOperationException("autoRebuild()");
  }

  public long rebuild(YTDatabaseSessionInternal session) {
    try (YTResultSet rs = getDatabase().command(String.format(QUERY_REBUILD, name))) {
      return rs.next().getProperty("totalIndexed");
    }
  }

  public OIndexRemote clear(YTDatabaseSessionInternal session) {
    getDatabase().command(String.format(QUERY_CLEAR, name)).close();
    return this;
  }

  public long getSize(YTDatabaseSessionInternal session) {
    try (YTResultSet result = getDatabase().indexQuery(name, String.format(QUERY_SIZE, name))) {
      if (result.hasNext()) {
        return result.next().getProperty("size");
      }
    }
    return 0;
  }

  public long getKeySize() {
    try (YTResultSet result = getDatabase().indexQuery(name, String.format(QUERY_KEY_SIZE, name))) {
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

  public YTEntityImpl getConfiguration(YTDatabaseSessionInternal session) {
    return configuration;
  }

  @Override
  public Map<String, ?> getMetadata() {
    var embedded = configuration.<YTEntityImpl>field("metadata", YTType.EMBEDDED);
    var map = embedded.toMap();

    map.remove("@rid");
    map.remove("@class");
    map.remove("@type");
    map.remove("@version");

    return map;
  }

  public YTRID getIdentity() {
    return rid;
  }

  public OIndexInternal getInternal() {
    return null;
  }

  public long rebuild(YTDatabaseSessionInternal session,
      final OProgressListener iProgressListener) {
    return rebuild(session);
  }

  public YTType[] getKeyTypes() {
    if (indexDefinition != null) {
      return indexDefinition.getTypes();
    }
    return new YTType[0];
  }

  public Collection<YTEntityImpl> getEntries(final Collection<?> iKeys) {
    final StringBuilder params = new StringBuilder(128);
    if (!iKeys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < iKeys.size(); i++) {
        params.append(", ?");
      }
    }

    try (YTResultSet rs =
        getDatabase()
            .indexQuery(name, String.format(QUERY_GET_ENTRIES, name, params), iKeys.toArray())) {
      return rs.stream().map((res) -> (YTEntityImpl) res.toEntity()).collect(Collectors.toList());
    }
  }

  public OIndexDefinition getDefinition() {
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

    final OIndexRemote that = (OIndexRemote) o;

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
  public Object getLastKey(YTDatabaseSessionInternal session) {
    throw new UnsupportedOperationException("getLastKey");
  }

  @Override
  public OIndexCursor iterateEntriesBetween(
      YTDatabaseSessionInternal session, Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesBetween");
  }

  @Override
  public OIndexCursor iterateEntriesMajor(YTDatabaseSessionInternal session, Object fromKey,
      boolean fromInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesMajor");
  }

  @Override
  public OIndexCursor iterateEntriesMinor(YTDatabaseSessionInternal session, Object toKey,
      boolean toInclusive, boolean ascOrder) {
    throw new UnsupportedOperationException("iterateEntriesMinor");
  }

  @Override
  public OIndexCursor iterateEntries(YTDatabaseSessionInternal session, Collection<?> keys,
      boolean ascSortOrder) {

    final StringBuilder params = new StringBuilder(128);
    if (!keys.isEmpty()) {
      params.append("?");
      for (int i = 1; i < keys.size(); i++) {
        params.append(", ?");
      }
    }

    final YTInternalResultSet copy = new YTInternalResultSet(); // TODO a raw array instead...?
    try (YTResultSet res =
        getDatabase()
            .indexQuery(
                name,
                String.format(QUERY_ITERATE_ENTRIES, name, params, ascSortOrder ? "ASC" : "DESC"),
                keys.toArray())) {

      res.forEachRemaining(x -> copy.add(x));
    }

    return new OIndexAbstractCursor() {

      @Override
      public Map.Entry<Object, YTIdentifiable> nextEntry() {
        if (!copy.hasNext()) {
          return null;
        }
        final YTResult next = copy.next();
        return new Map.Entry<Object, YTIdentifiable>() {
          @Override
          public Object getKey() {
            return next.getProperty("key");
          }

          @Override
          public YTIdentifiable getValue() {
            return next.getProperty("rid");
          }

          @Override
          public YTIdentifiable setValue(YTIdentifiable value) {
            throw new UnsupportedOperationException("cannot set value of index entry");
          }
        };
      }
    };
  }

  @Override
  public OIndexCursor cursor(YTDatabaseSessionInternal session) {
    final YTInternalResultSet copy = new YTInternalResultSet(); // TODO a raw array instead...?
    try (YTResultSet result = getDatabase().indexQuery(name, String.format(QUERY_ENTRIES, name))) {
      result.forEachRemaining(x -> copy.add(x));
    }

    return new OIndexAbstractCursor() {

      @Override
      public Map.Entry<Object, YTIdentifiable> nextEntry() {
        if (!copy.hasNext()) {
          return null;
        }

        final YTResult value = copy.next();

        return new Map.Entry<Object, YTIdentifiable>() {
          @Override
          public Object getKey() {
            return value.getProperty("key");
          }

          @Override
          public YTIdentifiable getValue() {
            return value.getProperty("rid");
          }

          @Override
          public YTIdentifiable setValue(YTIdentifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };
  }

  @Override
  public OIndexCursor descCursor(YTDatabaseSessionInternal session) {
    final YTInternalResultSet copy = new YTInternalResultSet(); // TODO a raw array instead...?
    try (YTResultSet result =
        getDatabase().indexQuery(name, String.format(QUERY_ENTRIES_DESC, name))) {
      result.forEachRemaining(x -> copy.add(x));
    }

    return new OIndexAbstractCursor() {

      @Override
      public Map.Entry<Object, YTIdentifiable> nextEntry() {
        if (!copy.hasNext()) {
          return null;
        }

        final YTResult value = copy.next();

        return new Map.Entry<Object, YTIdentifiable>() {
          @Override
          public Object getKey() {
            return value.getProperty("key");
          }

          @Override
          public YTIdentifiable getValue() {
            return value.getProperty("rid");
          }

          @Override
          public YTIdentifiable setValue(YTIdentifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    final YTInternalResultSet copy = new YTInternalResultSet(); // TODO a raw array instead...?
    try (final YTResultSet result =
        getDatabase().indexQuery(name, String.format(QUERY_KEYS, name))) {
      result.forEachRemaining(x -> copy.add(x));
    }
    return new OIndexKeyCursor() {

      @Override
      public Object next(int prefetchSize) {
        if (!copy.hasNext()) {
          return null;
        }

        final YTResult value = copy.next();

        return value.getProperty("key");
      }
    };
  }

  @Override
  public int compareTo(OIndex index) {
    final String name = index.getName();
    return this.name.compareTo(name);
  }

  protected YTDatabaseSessionInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }
}
