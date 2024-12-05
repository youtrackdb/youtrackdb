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
package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.common.listener.OProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema class
 */
public interface YTClass extends Comparable<YTClass> {

  String EDGE_CLASS_NAME = "E";
  String VERTEX_CLASS_NAME = "V";

  enum ATTRIBUTES {
    NAME,
    SHORTNAME,
    SUPERCLASS,
    SUPERCLASSES,
    OVERSIZE,
    STRICTMODE,
    ADDCLUSTER,
    REMOVECLUSTER,
    CUSTOM,
    ABSTRACT,
    CLUSTERSELECTION,
    DESCRIPTION,
    ENCRYPTION
  }

  enum INDEX_TYPE {
    UNIQUE(true),
    NOTUNIQUE(true),
    FULLTEXT(true),
    /**
     * @deprecated can be used only as manual index and manual indexes are deprecated and will be
     * removed
     */
    @Deprecated
    DICTIONARY(false),
    PROXY(true),
    UNIQUE_HASH_INDEX(true),
    NOTUNIQUE_HASH_INDEX(true),
    DICTIONARY_HASH_INDEX(false),
    SPATIAL(true);

    private final boolean automaticIndexable;

    INDEX_TYPE(boolean iValue) {
      automaticIndexable = iValue;
    }

    boolean isAutomaticIndexable() {
      return automaticIndexable;
    }
  }

  boolean isAbstract();

  YTClass setAbstract(YTDatabaseSession session, boolean iAbstract);

  boolean isStrictMode();

  YTClass setStrictMode(YTDatabaseSession session, boolean iMode);

  @Deprecated
  YTClass getSuperClass();

  @Deprecated
  YTClass setSuperClass(YTDatabaseSession session, YTClass iSuperClass);

  boolean hasSuperClasses();

  List<String> getSuperClassesNames();

  List<YTClass> getSuperClasses();

  YTClass setSuperClasses(YTDatabaseSession session, List<? extends YTClass> classes);

  YTClass addSuperClass(YTDatabaseSession session, YTClass superClass);

  YTClass removeSuperClass(YTDatabaseSession session, YTClass superClass);

  String getName();

  YTClass setName(YTDatabaseSession session, String iName);

  String getDescription();

  YTClass setDescription(YTDatabaseSession session, String iDescription);

  String getStreamableName();

  Collection<YTProperty> declaredProperties();

  Collection<YTProperty> properties(YTDatabaseSession session);

  Map<String, YTProperty> propertiesMap(YTDatabaseSession session);

  Collection<YTProperty> getIndexedProperties(YTDatabaseSession session);

  YTProperty getProperty(String iPropertyName);

  YTProperty createProperty(YTDatabaseSession session, String iPropertyName, YTType iType);

  YTProperty createProperty(YTDatabaseSession session, String iPropertyName, YTType iType,
      YTClass iLinkedClass);

  /**
   * Create a property in the class with the specified options.
   *
   * @param session
   * @param iPropertyName the name of the property.
   * @param iType         the type of the property.
   * @param iLinkedClass  in case of property of type
   *                      LINK,LINKLIST,LINKSET,LINKMAP,EMBEDDED,EMBEDDEDLIST,EMBEDDEDSET,EMBEDDEDMAP
   *                      can be specified a linked class in all the other cases should be null
   * @param iUnsafe       if true avoid to check the persistent data for compatibility, should be
   *                      used only if all persistent data is compatible with the property
   * @return the created property.
   */
  YTProperty createProperty(YTDatabaseSession session, String iPropertyName, YTType iType,
      YTClass iLinkedClass, boolean iUnsafe);

  YTProperty createProperty(YTDatabaseSession session, String iPropertyName, YTType iType,
      YTType iLinkedType);

  /**
   * Create a property in the class with the specified options.
   *
   * @param session
   * @param iPropertyName the name of the property.
   * @param iType         the type of the property.
   * @param iLinkedType   in case of property of type EMBEDDEDLIST,EMBEDDEDSET,EMBEDDEDMAP can be
   *                      specified a linked type in all the other cases should be null
   * @param iUnsafe       if true avoid to check the persistent data for compatibility, should be
   *                      used only if all persistent data is compatible with the property
   * @return the created property.
   */
  YTProperty createProperty(YTDatabaseSession session, String iPropertyName, YTType iType,
      YTType iLinkedType, boolean iUnsafe);

  void dropProperty(YTDatabaseSession session, String iPropertyName);

  boolean existsProperty(String iPropertyName);

  int getClusterForNewInstance(EntityImpl doc);

  int getDefaultClusterId();

  void setDefaultClusterId(YTDatabaseSession session, int iDefaultClusterId);

  int[] getClusterIds();

  YTClass addClusterId(YTDatabaseSession session, int iId);

  OClusterSelectionStrategy getClusterSelection();

  YTClass setClusterSelection(YTDatabaseSession session,
      OClusterSelectionStrategy clusterSelection);

  YTClass setClusterSelection(YTDatabaseSession session, String iStrategyName);

  YTClass addCluster(YTDatabaseSession session, String iClusterName);

  /**
   * Removes all data in the cluster with given name. As result indexes for this class will be
   * rebuilt.
   *
   * @param session
   * @param clusterName Name of cluster to be truncated.
   * @return Instance of current object.
   */
  YTClass truncateCluster(YTDatabaseSession session, String clusterName);

  YTClass removeClusterId(YTDatabaseSession session, int iId);

  int[] getPolymorphicClusterIds();

  @Deprecated
  Collection<YTClass> getBaseClasses();

  @Deprecated
  Collection<YTClass> getAllBaseClasses();

  /**
   * @return all the subclasses (one level hierarchy only)
   */
  Collection<YTClass> getSubclasses();

  /**
   * @return all the subclass hierarchy
   */
  Collection<YTClass> getAllSubclasses();

  /**
   * @return all recursively collected super classes
   */
  Collection<YTClass> getAllSuperClasses();

  long getSize(YTDatabaseSessionInternal session);

  float getClassOverSize();

  /**
   * Returns the oversize factor. Oversize is used to extend the record size by a factor to avoid
   * defragmentation upon updates. 0 or 1.0 means no oversize.
   *
   * @return Oversize factor
   * @see #setOverSize(YTDatabaseSession, float)
   */
  float getOverSize();

  /**
   * Sets the oversize factor. Oversize is used to extend the record size by a factor to avoid
   * defragmentation upon updates. 0 or 1.0 means no oversize. Default is 0.
   *
   * @return Oversize factor
   * @see #getOverSize()
   */
  YTClass setOverSize(YTDatabaseSession session, float overSize);

  /**
   * Returns the number of the records of this class considering also subclasses (polymorphic).
   */
  long count(YTDatabaseSession session);

  /**
   * Returns the number of the records of this class and based on polymorphic parameter it consider
   * or not the subclasses.
   */
  long count(YTDatabaseSession session, boolean iPolymorphic);

  /**
   * Truncates all the clusters the class uses.
   *
   * @throws IOException
   */
  void truncate(YTDatabaseSession session) throws IOException;

  /**
   * Tells if the current instance extends the passed schema class (iClass).
   *
   * @param iClassName
   * @return true if the current instance extends the passed schema class (iClass).
   * @see #isSuperClassOf(YTClass)
   */
  boolean isSubClassOf(String iClassName);

  /**
   * Returns true if the current instance extends the passed schema class (iClass).
   *
   * @param iClass
   * @return true if the current instance extends the passed schema class (iClass).
   * @see #isSuperClassOf(YTClass)
   */
  boolean isSubClassOf(YTClass iClass);

  /**
   * Returns true if the passed schema class (iClass) extends the current instance.
   *
   * @param iClass
   * @return Returns true if the passed schema class extends the current instance.
   * @see #isSubClassOf(YTClass)
   */
  boolean isSuperClassOf(YTClass iClass);

  String getShortName();

  YTClass setShortName(YTDatabaseSession session, String shortName);

  Object get(ATTRIBUTES iAttribute);

  YTClass set(YTDatabaseSession session, ATTRIBUTES attribute, Object iValue);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance and associated with database index.
   *
   * @param session
   * @param iName   Database index name
   * @param iType   Index type.
   * @param fields  Field names from which index will be created.
   * @return Class index registered inside of given class ans associated with database index.
   */
  OIndex createIndex(YTDatabaseSession session, String iName, INDEX_TYPE iType, String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance and associated with database index.
   *
   * @param session
   * @param iName   Database index name
   * @param iType   Index type.
   * @param fields  Field names from which index will be created.
   * @return Class index registered inside of given class ans associated with database index.
   */
  OIndex createIndex(YTDatabaseSession session, String iName, String iType, String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance.
   *
   * @param session
   * @param iName             Database index name.
   * @param iType             Index type.
   * @param iProgressListener Progress listener.
   * @param fields            Field names from which index will be created.
   * @return Class index registered inside of given class ans associated with database index.
   */
  OIndex createIndex(
      YTDatabaseSession session, String iName, INDEX_TYPE iType,
      OProgressListener iProgressListener,
      String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance.
   *
   * @param session
   * @param iName             Database index name.
   * @param iType             Index type.
   * @param iProgressListener Progress listener.
   * @param metadata          Additional parameters which will be added in index configuration
   *                          document as "metadata" field.
   * @param algorithm         Algorithm to use for indexing.
   * @param fields            Field names from which index will be created. @return Class index
   *                          registered inside of given class ans associated with database index.
   */
  OIndex createIndex(
      YTDatabaseSession session, String iName,
      String iType,
      OProgressListener iProgressListener,
      EntityImpl metadata,
      String algorithm,
      String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance.
   *
   * @param session
   * @param iName             Database index name.
   * @param iType             Index type.
   * @param iProgressListener Progress listener.
   * @param metadata          Additional parameters which will be added in index configuration
   *                          document as "metadata" field.
   * @param fields            Field names from which index will be created. @return Class index
   *                          registered inside of given class ans associated with database index.
   */
  OIndex createIndex(
      YTDatabaseSession session, String iName,
      String iType,
      OProgressListener iProgressListener,
      EntityImpl metadata,
      String... fields);

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of
   * fields does not matter.
   *
   * <p>All indexes sorted by their count of parameters in ascending order. If there are indexes
   * for the given set of fields in super class they will be taken into account.
   *
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * @see OIndexDefinition#getParamCount()
   */
  Set<OIndex> getInvolvedIndexes(YTDatabaseSession session, Collection<String> fields);

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of
   * fields does not matter.
   *
   * <p>All indexes sorted by their count of parameters in ascending order. If there are indexes
   * for the given set of fields in super class they will be taken into account.
   *
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * @see #getInvolvedIndexes(YTDatabaseSession, Collection)
   */
  Set<OIndex> getInvolvedIndexes(YTDatabaseSession session, String... fields);

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of
   * fields does not matter.
   *
   * <p>Indexes that related only to the given class will be returned.
   *
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * @see OIndexDefinition#getParamCount()
   */
  Set<OIndex> getClassInvolvedIndexes(YTDatabaseSession session, Collection<String> fields);

  /**
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * @see #getClassInvolvedIndexes(YTDatabaseSession, Collection)
   */
  Set<OIndex> getClassInvolvedIndexes(YTDatabaseSession session, String... fields);

  /**
   * Indicates whether given fields are contained as first key fields in class indexes. Order of
   * fields does not matter. If there are indexes for the given set of fields in super class they
   * will be taken into account.
   *
   * @param session
   * @param fields  Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   */
  boolean areIndexed(YTDatabaseSession session, Collection<String> fields);

  /**
   * @param session
   * @param fields  Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   * @see #areIndexed(YTDatabaseSession, Collection)
   */
  boolean areIndexed(YTDatabaseSession session, String... fields);

  /**
   * Returns index instance by database index name.
   *
   * @param session
   * @param iName   Database index name.
   * @return Index instance.
   */
  OIndex getClassIndex(YTDatabaseSession session, String iName);

  /**
   * @return All indexes for given class, not the inherited ones.
   */
  Set<OIndex> getClassIndexes(YTDatabaseSession session);

  /**
   * Internal. Copy all the indexes for given class, not the inherited ones, in the collection
   * received as argument.
   */
  void getClassIndexes(YTDatabaseSession session, Collection<OIndex> indexes);

  /**
   * Internal. All indexes for given class and its super classes.
   */
  void getIndexes(YTDatabaseSession session, Collection<OIndex> indexes);

  /**
   * @return All indexes for given class and its super classes.
   */
  Set<OIndex> getIndexes(YTDatabaseSession session);

  /**
   * Returns the auto sharding index configured for the class if any.
   */
  OIndex getAutoShardingIndex(YTDatabaseSession session);

  /**
   * @return true if this class represents a subclass of an edge class (E)
   */
  boolean isEdgeType();

  /**
   * @return true if this class represents a subclass of a vertex class (V)
   */
  boolean isVertexType();

  String getCustom(String iName);

  YTClass setCustom(YTDatabaseSession session, String iName, String iValue);

  void removeCustom(YTDatabaseSession session, String iName);

  void clearCustom(YTDatabaseSession session);

  Set<String> getCustomKeys();

  boolean hasClusterId(int clusterId);

  boolean hasPolymorphicClusterId(int clusterId);
}
