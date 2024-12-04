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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.security.OPropertyAccess;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityResourceProperty;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Interface to handle index.
 */
public interface OIndexInternal extends OIndex {

  String CONFIG_KEYTYPE = "keyType";
  String CONFIG_AUTOMATIC = "automatic";
  String CONFIG_TYPE = "type";
  String ALGORITHM = "algorithm";
  String VALUE_CONTAINER_ALGORITHM = "valueContainerAlgorithm";
  String CONFIG_NAME = "name";
  String INDEX_DEFINITION = "indexDefinition";
  String INDEX_DEFINITION_CLASS = "indexDefinitionClass";
  String INDEX_VERSION = "indexVersion";
  String METADATA = "metadata";
  String MERGE_KEYS = "mergeKeys";

  Object getCollatingValue(final Object key);

  /**
   * Loads the index giving the configuration.
   *
   * @param session
   * @param iConfig YTDocument instance containing the configuration
   */
  boolean loadFromConfiguration(YTDatabaseSessionInternal session, YTDocument iConfig);

  /**
   * Saves the index configuration to disk.
   *
   * @return The configuration as YTDocument instance
   * @see OIndex#getConfiguration(YTDatabaseSessionInternal)
   */
  YTDocument updateConfiguration(YTDatabaseSessionInternal session);

  /**
   * Add given cluster to the list of clusters that should be automatically indexed.
   *
   * @param session
   * @param iClusterName Cluster to add.
   * @return Current index instance.
   */
  OIndex addCluster(YTDatabaseSessionInternal session, final String iClusterName);

  /**
   * Remove given cluster from the list of clusters that should be automatically indexed.
   *
   * @param session
   * @param iClusterName Cluster to remove.
   */
  void removeCluster(YTDatabaseSessionInternal session, final String iClusterName);

  /**
   * Indicates whether given index can be used to calculate result of
   * {@link com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   *
   * @return {@code true} if given index can be used to calculate result of
   * {@link com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquality} operators.
   */
  boolean canBeUsedInEqualityOperators();

  boolean hasRangeQuerySupport();

  OIndexMetadata loadMetadata(YTDocument iConfig);

  void close();

  /**
   * Returns the index name for a key. The name is always the current index name, but in cases where
   * the index supports key-based sharding.
   *
   * @param key the index key.
   * @return The index name involved
   */
  String getIndexNameByKey(Object key);

  /**
   * Acquires exclusive lock in the active atomic operation running on the current thread for this
   * index.
   *
   * <p>If this index supports a more narrow locking, for example key-based sharding, it may use
   * the provided {@code key} to infer a more narrow lock scope, but that is not a requirement.
   *
   * @param key the index key to lock.
   * @return {@code true} if this index was locked entirely, {@code false} if this index locking is
   * sensitive to the provided {@code key} and only some subset of this index was locked.
   */
  boolean acquireAtomicExclusiveLock(Object key);

  /**
   * @return number of entries in the index.
   */
  long size(YTDatabaseSessionInternal session);

  Stream<YTRID> getRids(YTDatabaseSessionInternal session, final Object key);

  Stream<ORawPair<Object, YTRID>> stream(YTDatabaseSessionInternal session);

  Stream<ORawPair<Object, YTRID>> descStream(YTDatabaseSessionInternal session);

  Stream<Object> keyStream();

  /**
   * Returns stream which presents subset of index data between passed in keys.
   *
   * @param session
   * @param fromKey       Lower border of index data.
   * @param fromInclusive Indicates whether lower border should be inclusive or exclusive.
   * @param toKey         Upper border of index data.
   * @param toInclusive   Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder      Flag which determines whether data iterated by stream should be in
   *                      ascending or descending order.
   * @return Cursor which presents subset of index data between passed in keys.
   */
  Stream<ORawPair<Object, YTRID>> streamEntriesBetween(
      YTDatabaseSessionInternal session, Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder);

  /**
   * Returns stream which presents data associated with passed in keys.
   *
   * @param session
   * @param keys         Keys data of which should be returned.
   * @param ascSortOrder Flag which determines whether data iterated by stream should be in
   *                     ascending or descending order.
   * @return stream which presents data associated with passed in keys.
   */
  Stream<ORawPair<Object, YTRID>> streamEntries(YTDatabaseSessionInternal session,
      Collection<?> keys,
      boolean ascSortOrder);

  /**
   * Returns stream which presents subset of data which associated with key which is greater than
   * passed in key.
   *
   * @param session
   * @param fromKey       Lower border of index data.
   * @param fromInclusive Indicates whether lower border should be inclusive or exclusive.
   * @param ascOrder      Flag which determines whether data iterated by stream should be in
   *                      ascending or descending order.
   * @return stream which presents subset of data which associated with key which is greater than
   * passed in key.
   */
  Stream<ORawPair<Object, YTRID>> streamEntriesMajor(
      YTDatabaseSessionInternal session, Object fromKey, boolean fromInclusive, boolean ascOrder);

  /**
   * Returns stream which presents subset of data which associated with key which is less than
   * passed in key.
   *
   * @param session
   * @param toKey       Upper border of index data.
   * @param toInclusive Indicates Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder    Flag which determines whether data iterated by stream should be in ascending
   *                    or descending order.
   * @return stream which presents subset of data which associated with key which is less than
   * passed in key.
   */
  Stream<ORawPair<Object, YTRID>> streamEntriesMinor(
      YTDatabaseSessionInternal session, Object toKey, boolean toInclusive, boolean ascOrder);

  static YTIdentifiable securityFilterOnRead(OIndex idx, YTIdentifiable item) {
    if (idx.getDefinition() == null) {
      return item;
    }
    String indexClass = idx.getDefinition().getClassName();
    if (indexClass == null) {
      return item;
    }
    YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return item;
    }
    OSecurityInternal security = db.getSharedContext().getSecurity();
    if (isReadRestrictedBySecurityPolicy(indexClass, db, security)) {
      try {
        item = item.getRecord();
      } catch (YTRecordNotFoundException e) {
        item = null;
      }
    }
    if (item == null) {
      return null;
    }
    if (idx.getDefinition().getFields().size() == 1) {
      String indexProp = idx.getDefinition().getFields().get(0);
      if (isLabelSecurityDefined(db, security, indexClass, indexProp)) {
        try {
          item = item.getRecord();
        } catch (YTRecordNotFoundException e) {
          item = null;
        }
        if (item == null) {
          return null;
        }
        if (!(item instanceof YTDocument)) {
          return item;
        }
        OPropertyAccess access = ODocumentInternal.getPropertyAccess((YTDocument) item);
        if (access != null && !access.isReadable(indexProp)) {
          return null;
        }
      }
    }
    return item;
  }

  static boolean isLabelSecurityDefined(
      YTDatabaseSessionInternal database,
      OSecurityInternal security,
      String indexClass,
      String propertyName) {
    Set<String> classesToCheck = new HashSet<>();
    classesToCheck.add(indexClass);
    YTClass clazz = database.getClass(indexClass);
    if (clazz == null) {
      return false;
    }
    clazz.getAllSubclasses().forEach(x -> classesToCheck.add(x.getName()));
    clazz.getAllSuperClasses().forEach(x -> classesToCheck.add(x.getName()));
    Set<OSecurityResourceProperty> allFilteredProperties =
        security.getAllFilteredProperties(database);

    for (String className : classesToCheck) {
      Optional<OSecurityResourceProperty> item =
          allFilteredProperties.stream()
              .filter(x -> x.getClassName().equalsIgnoreCase(className))
              .filter(x -> x.getPropertyName().equals(propertyName))
              .findFirst();

      if (item.isPresent()) {
        return true;
      }
    }
    return false;
  }

  static boolean isReadRestrictedBySecurityPolicy(
      String indexClass, YTDatabaseSessionInternal db, OSecurityInternal security) {
    if (security.isReadRestrictedBySecurityPolicy(db, "database.class." + indexClass)) {
      return true;
    }

    YTClass clazz = db.getClass(indexClass);
    if (clazz != null) {
      Collection<YTClass> sub = clazz.getSubclasses();
      for (YTClass subClass : sub) {
        if (isReadRestrictedBySecurityPolicy(subClass.getName(), db, security)) {
          return true;
        }
      }
    }

    return false;
  }

  boolean isNativeTxSupported();

  Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes);

  void doPut(YTDatabaseSessionInternal session, OAbstractPaginatedStorage storage, Object key,
      YTRID rid)
      throws OInvalidIndexEngineIdException;

  boolean doRemove(YTDatabaseSessionInternal session, OAbstractPaginatedStorage storage, Object key,
      YTRID rid)
      throws OInvalidIndexEngineIdException;

  boolean doRemove(OAbstractPaginatedStorage storage, Object key)
      throws OInvalidIndexEngineIdException;

  Stream<YTRID> getRidsIgnoreTx(YTDatabaseSessionInternal session, Object key);

  OIndex create(YTDatabaseSessionInternal session, OIndexMetadata metadata, boolean rebuild,
      OProgressListener progressListener);

  int getIndexId();
}
