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
package com.orientechnologies.core.metadata.schema;

import com.orientechnologies.core.collate.OCollate;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.Collection;
import java.util.Set;

/**
 * Contains the description of a persistent class property.
 */
public interface YTProperty extends Comparable<YTProperty> {

  enum ATTRIBUTES {
    LINKEDTYPE,
    LINKEDCLASS,
    MIN,
    MAX,
    MANDATORY,
    NAME,
    NOTNULL,
    REGEXP,
    TYPE,
    CUSTOM,
    READONLY,
    COLLATE,
    DEFAULT,
    DESCRIPTION
  }

  String getName();

  /**
   * Returns the full name as <class>.<property>
   */
  String getFullName();

  YTProperty setName(YTDatabaseSession session, String iName);

  void set(YTDatabaseSession session, ATTRIBUTES attribute, Object iValue);

  YTType getType();

  /**
   * Returns the linked class in lazy mode because while unmarshalling the class could be not loaded
   * yet.
   *
   * @return
   */
  YTClass getLinkedClass();

  YTProperty setLinkedClass(YTDatabaseSession session, YTClass oClass);

  YTType getLinkedType();

  YTProperty setLinkedType(YTDatabaseSession session, YTType type);

  boolean isNotNull();

  YTProperty setNotNull(YTDatabaseSession session, boolean iNotNull);

  OCollate getCollate();

  YTProperty setCollate(YTDatabaseSession session, String iCollateName);

  YTProperty setCollate(YTDatabaseSession session, OCollate collate);

  boolean isMandatory();

  YTProperty setMandatory(YTDatabaseSession session, boolean mandatory);

  boolean isReadonly();

  YTProperty setReadonly(YTDatabaseSession session, boolean iReadonly);

  /**
   * Min behavior depends on the Property YTType.
   *
   * <p>
   *
   * <ul>
   *   <li>String : minimum length
   *   <li>Number : minimum value
   *   <li>date and time : minimum time in millisecond, date must be written in the storage date
   *       format
   *   <li>binary : minimum size of the byte array
   *   <li>List,Set,Collection : minimum size of the collection
   * </ul>
   *
   * @return String, can be null
   */
  String getMin();

  /**
   * @param session
   * @param min     can be null
   * @return this property
   * @see YTProperty#getMin()
   */
  YTProperty setMin(YTDatabaseSession session, String min);

  /**
   * Max behavior depends on the Property YTType.
   *
   * <p>
   *
   * <ul>
   *   <li>String : maximum length
   *   <li>Number : maximum value
   *   <li>date and time : maximum time in millisecond, date must be written in the storage date
   *       format
   *   <li>binary : maximum size of the byte array
   *   <li>List,Set,Collection : maximum size of the collection
   * </ul>
   *
   * @return String, can be null
   */
  String getMax();

  /**
   * @param session
   * @param max     can be null
   * @return this property
   * @see YTProperty#getMax()
   */
  YTProperty setMax(YTDatabaseSession session, String max);

  /**
   * Default value for the property; can be function
   *
   * @return String, can be null
   */
  String getDefaultValue();

  /**
   * @param session
   * @param defaultValue can be null
   * @return this property
   * @see YTProperty#getDefaultValue()
   */
  YTProperty setDefaultValue(YTDatabaseSession session, String defaultValue);

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param session
   * @param iType   One of types supported.
   *                <ul>
   *                  <li>UNIQUE: Doesn't allow duplicates
   *                  <li>NOTUNIQUE: Allow duplicates
   *                  <li>FULLTEXT: Indexes single word for full text search
   *                </ul>
   * @return see
   * {@link YTClass#createIndex(YTDatabaseSession, String,
   * YTClass.INDEX_TYPE, String...)}.
   */
  OIndex createIndex(YTDatabaseSession session, final INDEX_TYPE iType);

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param session
   * @param iType
   * @return see
   * {@link YTClass#createIndex(YTDatabaseSession, String,
   * YTClass.INDEX_TYPE, String...)}.
   */
  OIndex createIndex(YTDatabaseSession session, final String iType);

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param session
   * @param iType    One of types supported.
   *                 <ul>
   *                   <li>UNIQUE: Doesn't allow duplicates
   *                   <li>NOTUNIQUE: Allow duplicates
   *                   <li>FULLTEXT: Indexes single word for full text search
   *                 </ul>
   * @param metadata the index metadata
   * @return see
   * {@link YTClass#createIndex(YTDatabaseSession, String,
   * YTClass.INDEX_TYPE, String...)}.
   */
  OIndex createIndex(YTDatabaseSession session, String iType, YTEntityImpl metadata);

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param session
   * @param iType    One of types supported.
   *                 <ul>
   *                   <li>UNIQUE: Doesn't allow duplicates
   *                   <li>NOTUNIQUE: Allow duplicates
   *                   <li>FULLTEXT: Indexes single word for full text search
   *                 </ul>
   * @param metadata the index metadata
   * @return see
   * {@link YTClass#createIndex(YTDatabaseSession, String,
   * YTClass.INDEX_TYPE, String...)}.
   */
  OIndex createIndex(YTDatabaseSession session, INDEX_TYPE iType, YTEntityImpl metadata);

  /**
   * Remove the index on property
   *
   * @return
   * @deprecated Use SQL command instead.
   */
  @Deprecated
  YTProperty dropIndexes(YTDatabaseSessionInternal session);

  /**
   * @return All indexes in which this property participates as first key item.
   * @deprecated Use
   * {@link YTClass#getInvolvedIndexes(YTDatabaseSession,
   * String...)} instead.
   */
  @Deprecated
  Set<OIndex> getIndexes(YTDatabaseSession session);

  /**
   * @return The first index in which this property participates as first key item.
   * @deprecated Use
   * {@link YTClass#getInvolvedIndexes(YTDatabaseSession,
   * String...)} instead.
   */
  @Deprecated
  OIndex getIndex(YTDatabaseSession session);

  /**
   * @return All indexes in which this property participates.
   */
  Collection<OIndex> getAllIndexes(YTDatabaseSession session);

  /**
   * Indicates whether property is contained in indexes as its first key item. If you would like to
   * fetch all indexes or check property presence in other indexes use
   * {@link #getAllIndexes(YTDatabaseSession)} instead.
   *
   * @return <code>true</code> if and only if this property is contained in indexes as its first key
   * item.
   * @deprecated Use
   * {@link YTClass#areIndexed(YTDatabaseSession, String...)}
   * instead.
   */
  @Deprecated
  boolean isIndexed(YTDatabaseSession session);

  String getRegexp();

  YTProperty setRegexp(YTDatabaseSession session, String regexp);

  /**
   * Change the type. It checks for compatibility between the change of type.
   *
   * @param session
   * @param iType
   */
  YTProperty setType(YTDatabaseSession session, final YTType iType);

  String getCustom(final String iName);

  YTProperty setCustom(YTDatabaseSession session, final String iName, final String iValue);

  void removeCustom(YTDatabaseSession session, final String iName);

  void clearCustom(YTDatabaseSession session);

  Set<String> getCustomKeys();

  YTClass getOwnerClass();

  Object get(ATTRIBUTES iAttribute);

  Integer getId();

  String getDescription();

  YTProperty setDescription(YTDatabaseSession session, String iDescription);
}
