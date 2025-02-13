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
package com.jetbrains.youtrack.db.api.schema;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema class
 */
public interface SchemaClass {

  String EDGE_CLASS_NAME = "E";
  String VERTEX_CLASS_NAME = "V";

  enum ATTRIBUTES {
    NAME,
    SHORTNAME,
    SUPERCLASS,
    SUPERCLASSES,
    STRICT_MODE,
    ADD_CLUSTER,
    REMOVE_CLUSTER,
    CUSTOM,
    ABSTRACT,
    CLUSTER_SELECTION,
    DESCRIPTION
  }

  enum INDEX_TYPE {
    UNIQUE,
    NOTUNIQUE,
    FULLTEXT,
    SPATIAL
  }

  boolean isAbstract(DatabaseSession session);

  SchemaClass setAbstract(DatabaseSession session, boolean iAbstract);

  boolean isStrictMode(DatabaseSession session);

  void setStrictMode(DatabaseSession session, boolean iMode);

  @Deprecated
  SchemaClass getSuperClass(DatabaseSession session);

  @Deprecated
  SchemaClass setSuperClass(DatabaseSession session, SchemaClass iSuperClass);

  boolean hasSuperClasses(DatabaseSession session);

  List<String> getSuperClassesNames(DatabaseSession session);

  List<SchemaClass> getSuperClasses(DatabaseSession session);

  SchemaClass setSuperClasses(DatabaseSession session, List<? extends SchemaClass> classes);

  SchemaClass addSuperClass(DatabaseSession session, SchemaClass superClass);

  void removeSuperClass(DatabaseSession session, SchemaClass superClass);

  String getName(DatabaseSession session);

  SchemaClass setName(DatabaseSession session, String iName);

  String getDescription(DatabaseSession session);

  SchemaClass setDescription(DatabaseSession session, String iDescription);

  String getStreamableName(DatabaseSession session);

  Collection<SchemaProperty> declaredProperties(DatabaseSession session);

  Collection<SchemaProperty> properties(DatabaseSession session);

  Map<String, SchemaProperty> propertiesMap(DatabaseSession session);

  Collection<SchemaProperty> getIndexedProperties(DatabaseSession session);

  SchemaProperty getProperty(DatabaseSession session, String iPropertyName);

  SchemaProperty createProperty(DatabaseSession session, String iPropertyName, PropertyType iType);

  /**
   * Create a property in the class with the specified options.
   *
   * @param iPropertyName the name of the property.
   * @param iType         the type of the property.
   * @param iLinkedClass  in case of property of type
   *                      LINK,LINKLIST,LINKSET,LINKMAP,EMBEDDED,EMBEDDEDLIST,EMBEDDEDSET,EMBEDDEDMAP
   *                      can be specified a linked class in all the other cases should be null
   * @return the created property.
   */
  SchemaProperty createProperty(DatabaseSession session, String iPropertyName, PropertyType iType,
      SchemaClass iLinkedClass);

  /**
   * Create a property in the class with the specified options.
   *
   * @param iPropertyName the name of the property.
   * @param iType         the type of the property.
   * @param iLinkedType   in case of property of type EMBEDDEDLIST,EMBEDDEDSET,EMBEDDEDMAP can be
   *                      specified a linked type in all the other cases should be null
   * @return the created property.
   */
  SchemaProperty createProperty(DatabaseSession session, String iPropertyName, PropertyType iType,
      PropertyType iLinkedType);

  void dropProperty(DatabaseSession session, String iPropertyName);

  boolean existsProperty(DatabaseSession session, String iPropertyName);

  int[] getClusterIds(DatabaseSession session);

  SchemaClass addClusterId(DatabaseSession session, int iId);

  SchemaClass setClusterSelection(DatabaseSession session, String iStrategyName);

  String getClusterSelectionStrategyName(DatabaseSession session);

  SchemaClass addCluster(DatabaseSession session, String iClusterName);

  SchemaClass removeClusterId(DatabaseSession session, int iId);

  int[] getPolymorphicClusterIds(DatabaseSession session);

  /**
   * @return all the subclasses (one level hierarchy only)
   */
  Collection<SchemaClass> getSubclasses(DatabaseSession session);

  /**
   * @return all the subclass hierarchy
   */
  Collection<SchemaClass> getAllSubclasses(DatabaseSession session);

  /**
   * @return all recursively collected super classes
   */
  Collection<SchemaClass> getAllSuperClasses();

  /**
   * Tells if the current instance extends the passed schema class (iClass).
   *
   * @return true if the current instance extends the passed schema class (iClass).
   * @see #isSuperClassOf(DatabaseSession, SchemaClass)
   */
  boolean isSubClassOf(DatabaseSession session, String iClassName);

  /**
   * Returns true if the current instance extends the passed schema class (iClass).
   *
   * @return true if the current instance extends the passed schema class (iClass).
   * @see #isSuperClassOf(DatabaseSession, SchemaClass)
   */
  boolean isSubClassOf(DatabaseSession session, SchemaClass iClass);

  /**
   * Returns true if the passed schema class (iClass) extends the current instance.
   *
   * @return Returns true if the passed schema class extends the current instance.
   * @see #isSubClassOf(DatabaseSession, SchemaClass)
   */
  boolean isSuperClassOf(DatabaseSession session, SchemaClass iClass);

  String getShortName(DatabaseSession session);

  SchemaClass setShortName(DatabaseSession session, String shortName);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance and associated with database index.
   *
   * @param iName   Database index name
   * @param iType   Index type.
   * @param fields  Field names from which index will be created.
   */
  void createIndex(DatabaseSession session, String iName, INDEX_TYPE iType, String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance and associated with database index.
   *
   * @param iName   Database index name
   * @param iType   Index type.
   * @param fields  Field names from which index will be created.
   */
  void createIndex(DatabaseSession session, String iName, String iType, String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance.
   *
   * @param iName             Database index name.
   * @param iType             Index type.
   * @param iProgressListener Progress listener.
   * @param fields            Field names from which index will be created.
   */
  void createIndex(
      DatabaseSession session, String iName, INDEX_TYPE iType,
      ProgressListener iProgressListener,
      String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance.
   *
   * @param iName             Database index name.
   * @param iType             Index type.
   * @param iProgressListener Progress listener.
   * @param metadata          Additional parameters which will be added in index configuration
   *                          document as "metadata" field.
   * @param algorithm         Algorithm to use for indexing.
   * @param fields            Field names from which index will be created. @return Class index
   *                          registered inside of given class ans associated with database index.
   */
  void createIndex(
      DatabaseSession session, String iName,
      String iType,
      ProgressListener iProgressListener,
      Map<String, ?> metadata,
      String algorithm,
      String... fields);

  /**
   * Creates database index that is based on passed in field names. Given index will be added into
   * class instance.
   *
   * @param iName             Database index name.
   * @param iType             Index type.
   * @param iProgressListener Progress listener.
   * @param metadata          Additional parameters which will be added in index configuration
   *                          document as "metadata" field.
   * @param fields            Field names from which index will be created. @return Class index
   *                          registered inside of given class ans associated with database index.
   */
  void createIndex(
      DatabaseSession session, String iName,
      String iType,
      ProgressListener iProgressListener,
      Map<String, ?> metadata,
      String... fields);

  /**
   * @return true if this class represents a subclass of an edge class (E)
   */
  boolean isEdgeType(DatabaseSession session);

  /**
   * @return true if this class represents a subclass of a vertex class (V)
   */
  boolean isVertexType(DatabaseSession session);

  String getCustom(DatabaseSession session, String iName);

  SchemaClass setCustom(DatabaseSession session, String iName, String iValue);

  void removeCustom(DatabaseSession session, String iName);

  void clearCustom(DatabaseSession session);

  Set<String> getCustomKeys(DatabaseSession session);

  boolean hasClusterId(int clusterId);

  boolean hasPolymorphicClusterId(int clusterId);
}
