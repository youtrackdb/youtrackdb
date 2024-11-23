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
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Set;

/**
 * Basic interface to handle index.
 */
public interface OIndex extends Comparable<OIndex> {

  String getDatabaseName();

  /**
   * Types of the keys that index can accept, if index contains composite key, list of types of
   * elements from which this index consist will be returned, otherwise single element (key type
   * obviously) will be returned.
   */
  OType[] getKeyTypes();

  /**
   * Gets the set of records associated with the passed key.
   *
   * @param session
   * @param key     The key to search
   * @return The Record set if found, otherwise an empty Set
   * @deprecated Use {@link OIndexInternal#getRids(ODatabaseSessionInternal, Object)} instead, but
   * only as internal (not public) API.
   */
  @Deprecated
  Object get(ODatabaseSessionInternal session, Object key);

  /**
   * Inserts a new entry in the index. The behaviour depends by the index implementation.
   *
   * @param session
   * @param key     Entry's key
   * @param value   Entry's value as OIdentifiable instance
   * @return The index instance itself to allow in chain calls
   */
  OIndex put(ODatabaseSessionInternal session, Object key, OIdentifiable value);

  /**
   * Removes an entry by its key.
   *
   * @param session
   * @param key     The entry's key to remove
   * @return True if the entry has been found and removed, otherwise false
   */
  boolean remove(ODatabaseSessionInternal session, Object key);

  /**
   * Removes an entry by its key and value.
   *
   * @param session
   * @param key     The entry's key to remove
   * @return True if the entry has been found and removed, otherwise false
   */
  boolean remove(ODatabaseSessionInternal session, Object key, OIdentifiable rid);

  /**
   * Clears the index removing all the entries in one shot.
   *
   * @return The index instance itself to allow in chain calls
   * @deprecated Manual indexes are deprecated and will be removed
   */
  @Deprecated
  OIndex clear(ODatabaseSessionInternal session);

  /**
   * @return number of entries in the index.
   * @deprecated Use {@link OIndexInternal#size(ODatabaseSessionInternal)} instead. This API only
   * for internal use !.
   */
  @Deprecated
  long getSize(ODatabaseSessionInternal session);

  /**
   * Counts the entries for the key.
   *
   * @deprecated Use <code>index.getInternal().getRids(key).count()</code> instead. This API only
   * for internal use !.
   */
  @Deprecated
  long count(ODatabaseSessionInternal session, Object iKey);

  /**
   * @return Number of keys in index
   * @deprecated Use <code>index.getInternal().getRids(key).distinct().count()</code> instead. This
   * API only for internal use !.
   */
  @Deprecated
  long getKeySize();

  /**
   * Flushes in-memory changes to disk.
   */
  @Deprecated
  void flush();

  @Deprecated
  long getRebuildVersion();

  /**
   * @return Indicates whether index is rebuilding at the moment.
   * @see #getRebuildVersion()
   */
  @Deprecated
  boolean isRebuilding();

  /**
   * @deprecated Use <code>index.getInternal().stream().findFirst().map(pair->pair.first)</code>
   * instead. This API only for internal use !
   */
  @Deprecated
  Object getFirstKey();

  /**
   * @deprecated Use <code>index.getInternal().descStream().findFirst().map(pair->pair.first)</code>
   * instead. This API only for internal use !
   */
  @Deprecated
  Object getLastKey(ODatabaseSessionInternal session);

  /**
   * @deprecated Use <code>index.getInternal().stream()</code> instead. This API only for internal
   * use !
   */
  @Deprecated
  OIndexCursor cursor(ODatabaseSessionInternal session);

  /**
   * @deprecated Use <code>index.getInternal().descStream()</code> instead. This API only for
   * internal use !
   */
  @Deprecated
  OIndexCursor descCursor(ODatabaseSessionInternal session);

  /**
   * @deprecated Use <code>index.getInternal().keyStream()</code> instead. This API only for
   * internal use !
   */
  @Deprecated
  OIndexKeyCursor keyCursor();

  /**
   * Delete the index.
   *
   * @return The index instance itself to allow in chain calls
   */
  OIndex delete(ODatabaseSessionInternal session);

  /**
   * Returns the index name.
   *
   * @return The name of the index
   */
  String getName();

  /**
   * Returns the type of the index as string.
   */
  String getType();

  /**
   * Returns the engine of the index as string.
   */
  String getAlgorithm();

  /**
   * Returns binary format version for this index. Index format changes during system development
   * but old formats are supported for binary compatibility. This method may be used to detect
   * version of binary format which is used by current index and upgrade index to new one.
   *
   * @return Returns binary format version for this index if possible, otherwise -1.
   */
  int getVersion();

  /**
   * Tells if the index is automatic. Automatic means it's maintained automatically by OxygenDB.
   * This is the case of indexes created against schema properties. Automatic indexes can always
   * been rebuilt.
   *
   * @return True if the index is automatic, otherwise false
   */
  boolean isAutomatic();

  /**
   * Rebuilds an automatic index.
   *
   * @return The number of entries rebuilt
   */
  long rebuild(ODatabaseSessionInternal session);

  /**
   * Populate the index with all the existent records.
   */
  long rebuild(ODatabaseSessionInternal session, OProgressListener iProgressListener);

  /**
   * Returns the index configuration.
   *
   * @return An ODocument object containing all the index properties
   */
  ODocument getConfiguration();

  /**
   * Returns the internal index used.
   */
  OIndexInternal getInternal();

  OIndexDefinition getDefinition();

  /**
   * Returns Names of clusters that will be indexed.
   *
   * @return Names of clusters that will be indexed.
   */
  Set<String> getClusters();

  /**
   * Returns cursor which presents data associated with passed in keys.
   *
   * @param session
   * @param keys         Keys data of which should be returned.
   * @param ascSortOrder Flag which determines whether data iterated by cursor should be in
   *                     ascending or descending order.
   * @return cursor which presents data associated with passed in keys.
   * @deprecated Use
   * {@link OIndexInternal#streamEntries(ODatabaseSessionInternal, Collection, boolean)} instead.
   * This API only for internal use !
   */
  @Deprecated
  OIndexCursor iterateEntries(ODatabaseSessionInternal session, Collection<?> keys,
      boolean ascSortOrder);

  /**
   * Returns cursor which presents subset of index data between passed in keys.
   *
   * @param session
   * @param fromKey       Lower border of index data.
   * @param fromInclusive Indicates whether lower border should be inclusive or exclusive.
   * @param toKey         Upper border of index data.
   * @param toInclusive   Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder      Flag which determines whether data iterated by cursor should be in
   *                      ascending or descending order.
   * @return Cursor which presents subset of index data between passed in keys.
   * @deprecated Use
   * {@link OIndexInternal#streamEntriesBetween(ODatabaseSessionInternal, Object, boolean, Object,
   * boolean, boolean)} instead. This API only * for internal use !
   */
  @Deprecated
  OIndexCursor iterateEntriesBetween(
      ODatabaseSessionInternal session, Object fromKey, boolean fromInclusive, Object toKey,
      boolean toInclusive, boolean ascOrder);

  /**
   * Returns cursor which presents subset of data which associated with key which is greater than
   * passed in key.
   *
   * @param session
   * @param fromKey       Lower border of index data.
   * @param fromInclusive Indicates whether lower border should be inclusive or exclusive.
   * @param ascOrder      Flag which determines whether data iterated by cursor should be in
   *                      ascending or descending order.
   * @return cursor which presents subset of data which associated with key which is greater than
   * passed in key.
   * @deprecated Use
   * {@link OIndexInternal#streamEntriesMajor(ODatabaseSessionInternal, Object, boolean, boolean)}
   * instead. This API only for internal use !
   */
  @Deprecated
  OIndexCursor iterateEntriesMajor(ODatabaseSessionInternal session, Object fromKey,
      boolean fromInclusive, boolean ascOrder);

  /**
   * Returns cursor which presents subset of data which associated with key which is less than
   * passed in key.
   *
   * @param session
   * @param toKey       Upper border of index data.
   * @param toInclusive Indicates Indicates whether upper border should be inclusive or exclusive.
   * @param ascOrder    Flag which determines whether data iterated by cursor should be in ascending
   *                    or descending order.
   * @return cursor which presents subset of data which associated with key which is less than
   * passed in key.
   * @deprecated Use
   * {@link OIndexInternal#streamEntriesMinor(ODatabaseSessionInternal, Object, boolean, boolean)}
   * instead. This API only for internal use !
   */
  @Deprecated
  OIndexCursor iterateEntriesMinor(ODatabaseSessionInternal session, Object toKey,
      boolean toInclusive, boolean ascOrder);

  ODocument getMetadata();

  boolean supportsOrderedIterations();

  boolean isUnique();
}
