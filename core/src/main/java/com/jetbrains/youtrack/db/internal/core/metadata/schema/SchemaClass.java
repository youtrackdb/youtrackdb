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

import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection.ClusterSelectionStrategy;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema class
 */
public interface SchemaClass extends Comparable<SchemaClass> {

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

  SchemaClass setAbstract(DatabaseSession session, boolean iAbstract);

  boolean isStrictMode();

  SchemaClass setStrictMode(DatabaseSession session, boolean iMode);

  @Deprecated
  SchemaClass getSuperClass();

  @Deprecated
  SchemaClass setSuperClass(DatabaseSession session, SchemaClass iSuperClass);

  boolean hasSuperClasses();

  List<String> getSuperClassesNames();

  List<SchemaClass> getSuperClasses();

  SchemaClass setSuperClasses(DatabaseSession session, List<? extends SchemaClass> classes);

  SchemaClass addSuperClass(DatabaseSession session, SchemaClass superClass);

  SchemaClass removeSuperClass(DatabaseSession session, SchemaClass superClass);

  String getName();

  SchemaClass setName(DatabaseSession session, String iName);

  String getDescription();

  SchemaClass setDescription(DatabaseSession session, String iDescription);

  String getStreamableName();

  Collection<Property> declaredProperties();

  Collection<Property> properties(DatabaseSession session);

  Map<String, Property> propertiesMap(DatabaseSession session);

  Collection<Property> getIndexedProperties(DatabaseSession session);

  Property getProperty(String iPropertyName);

  Property createProperty(DatabaseSession session, String iPropertyName, PropertyType iType);

  Property createProperty(DatabaseSession session, String iPropertyName, PropertyType iType,
      SchemaClass iLinkedClass);

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
  Property createProperty(DatabaseSession session, String iPropertyName, PropertyType iType,
      SchemaClass iLinkedClass, boolean iUnsafe);

  Property createProperty(DatabaseSession session, String iPropertyName, PropertyType iType,
      PropertyType iLinkedType);

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
  Property createProperty(DatabaseSession session, String iPropertyName, PropertyType iType,
      PropertyType iLinkedType, boolean iUnsafe);

  void dropProperty(DatabaseSession session, String iPropertyName);

  boolean existsProperty(String iPropertyName);

  int getClusterForNewInstance(EntityImpl doc);

  int getDefaultClusterId();

  void setDefaultClusterId(DatabaseSession session, int iDefaultClusterId);

  int[] getClusterIds();

  SchemaClass addClusterId(DatabaseSession session, int iId);

  ClusterSelectionStrategy getClusterSelection();

  SchemaClass setClusterSelection(DatabaseSession session,
      ClusterSelectionStrategy clusterSelection);

  SchemaClass setClusterSelection(DatabaseSession session, String iStrategyName);

  SchemaClass addCluster(DatabaseSession session, String iClusterName);

  /**
   * Removes all data in the cluster with given name. As result indexes for this class will be
   * rebuilt.
   *
   * @param session
   * @param clusterName Name of cluster to be truncated.
   * @return Instance of current object.
   */
  SchemaClass truncateCluster(DatabaseSession session, String clusterName);

  SchemaClass removeClusterId(DatabaseSession session, int iId);

  int[] getPolymorphicClusterIds();

  @Deprecated
  Collection<SchemaClass> getBaseClasses();

  @Deprecated
  Collection<SchemaClass> getAllBaseClasses();

  /**
   * @return all the subclasses (one level hierarchy only)
   */
  Collection<SchemaClass> getSubclasses();

  /**
   * @return all the subclass hierarchy
   */
  Collection<SchemaClass> getAllSubclasses();

  /**
   * @return all recursively collected super classes
   */
  Collection<SchemaClass> getAllSuperClasses();

  long getSize(DatabaseSessionInternal session);

  float getClassOverSize();

  /**
   * Returns the oversize factor. Oversize is used to extend the record size by a factor to avoid
   * defragmentation upon updates. 0 or 1.0 means no oversize.
   *
   * @return Oversize factor
   * @see #setOverSize(DatabaseSession, float)
   */
  float getOverSize();

  /**
   * Sets the oversize factor. Oversize is used to extend the record size by a factor to avoid
   * defragmentation upon updates. 0 or 1.0 means no oversize. Default is 0.
   *
   * @return Oversize factor
   * @see #getOverSize()
   */
  SchemaClass setOverSize(DatabaseSession session, float overSize);

  /**
   * Returns the number of the records of this class considering also subclasses (polymorphic).
   */
  long count(DatabaseSession session);

  /**
   * Returns the number of the records of this class and based on polymorphic parameter it consider
   * or not the subclasses.
   */
  long count(DatabaseSession session, boolean iPolymorphic);

  /**
   * Truncates all the clusters the class uses.
   *
   * @throws IOException
   */
  void truncate(DatabaseSession session) throws IOException;

  /**
   * Tells if the current instance extends the passed schema class (iClass).
   *
   * @param iClassName
   * @return true if the current instance extends the passed schema class (iClass).
   * @see #isSuperClassOf(SchemaClass)
   */
  boolean isSubClassOf(String iClassName);

  /**
   * Returns true if the current instance extends the passed schema class (iClass).
   *
   * @param iClass
   * @return true if the current instance extends the passed schema class (iClass).
   * @see #isSuperClassOf(SchemaClass)
   */
  boolean isSubClassOf(SchemaClass iClass);

  /**
   * Returns true if the passed schema class (iClass) extends the current instance.
   *
   * @param iClass
   * @return Returns true if the passed schema class extends the current instance.
   * @see #isSubClassOf(SchemaClass)
   */
  boolean isSuperClassOf(SchemaClass iClass);

  String getShortName();

  SchemaClass setShortName(DatabaseSession session, String shortName);

  Object get(ATTRIBUTES iAttribute);

  SchemaClass set(DatabaseSession session, ATTRIBUTES attribute, Object iValue);

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
  Index createIndex(DatabaseSession session, String iName, INDEX_TYPE iType, String... fields);

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
  Index createIndex(DatabaseSession session, String iName, String iType, String... fields);

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
  Index createIndex(
      DatabaseSession session, String iName, INDEX_TYPE iType,
      ProgressListener iProgressListener,
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
  Index createIndex(
      DatabaseSession session, String iName,
      String iType,
      ProgressListener iProgressListener,
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
  Index createIndex(
      DatabaseSession session, String iName,
      String iType,
      ProgressListener iProgressListener,
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
   * @see IndexDefinition#getParamCount()
   */
  Set<Index> getInvolvedIndexes(DatabaseSession session, Collection<String> fields);

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
   * @see #getInvolvedIndexes(DatabaseSession, Collection)
   */
  Set<Index> getInvolvedIndexes(DatabaseSession session, String... fields);

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of
   * fields does not matter.
   *
   * <p>Indexes that related only to the given class will be returned.
   *
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * @see IndexDefinition#getParamCount()
   */
  Set<Index> getClassInvolvedIndexes(DatabaseSession session, Collection<String> fields);

  /**
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * @see #getClassInvolvedIndexes(DatabaseSession, Collection)
   */
  Set<Index> getClassInvolvedIndexes(DatabaseSession session, String... fields);

  /**
   * Indicates whether given fields are contained as first key fields in class indexes. Order of
   * fields does not matter. If there are indexes for the given set of fields in super class they
   * will be taken into account.
   *
   * @param session
   * @param fields  Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   */
  boolean areIndexed(DatabaseSession session, Collection<String> fields);

  /**
   * @param session
   * @param fields  Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   * @see #areIndexed(DatabaseSession, Collection)
   */
  boolean areIndexed(DatabaseSession session, String... fields);

  /**
   * Returns index instance by database index name.
   *
   * @param session
   * @param iName   Database index name.
   * @return Index instance.
   */
  Index getClassIndex(DatabaseSession session, String iName);

  /**
   * @return All indexes for given class, not the inherited ones.
   */
  Set<Index> getClassIndexes(DatabaseSession session);

  /**
   * Internal. Copy all the indexes for given class, not the inherited ones, in the collection
   * received as argument.
   */
  void getClassIndexes(DatabaseSession session, Collection<Index> indexes);

  /**
   * Internal. All indexes for given class and its super classes.
   */
  void getIndexes(DatabaseSession session, Collection<Index> indexes);

  /**
   * @return All indexes for given class and its super classes.
   */
  Set<Index> getIndexes(DatabaseSession session);

  /**
   * Returns the auto sharding index configured for the class if any.
   */
  Index getAutoShardingIndex(DatabaseSession session);

  /**
   * @return true if this class represents a subclass of an edge class (E)
   */
  boolean isEdgeType();

  /**
   * @return true if this class represents a subclass of a vertex class (V)
   */
  boolean isVertexType();

  String getCustom(String iName);

  SchemaClass setCustom(DatabaseSession session, String iName, String iValue);

  void removeCustom(DatabaseSession session, String iName);

  void clearCustom(DatabaseSession session);

  Set<String> getCustomKeys();

  boolean hasClusterId(int clusterId);

  boolean hasPolymorphicClusterId(int clusterId);
}
