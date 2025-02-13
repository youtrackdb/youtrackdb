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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ClassTrigger;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.function.FunctionLibraryImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityPolicy;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityShared;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.schedule.ScheduledEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaImmutableClass implements SchemaClassInternal {

  /**
   * use SchemaClass.EDGE_CLASS_NAME instead
   */
  @Deprecated
  public static final String EDGE_CLASS_NAME = SchemaClass.EDGE_CLASS_NAME;

  /**
   * use SchemaClass.EDGE_CLASS_NAME instead
   */
  @Deprecated
  public static final String VERTEX_CLASS_NAME = SchemaClass.VERTEX_CLASS_NAME;

  private boolean inited = false;
  private final boolean isAbstract;
  private final boolean strictMode;
  private final String name;
  private final String streamAbleName;
  private final Map<String, SchemaPropertyInternal> properties;
  private Map<String, SchemaProperty> allPropertiesMap;
  private Collection<SchemaProperty> allProperties;
  private final ClusterSelectionStrategy clusterSelection;
  private final int[] clusterIds;
  private final int[] polymorphicClusterIds;
  private final Collection<String> baseClassesNames;
  private final List<String> superClassesNames;

  private final String shortName;
  private final Map<String, String> customFields;
  private final String description;

  private final ImmutableSchema schema;
  // do not do it volatile it is already SAFE TO USE IT in MT mode.
  private final List<SchemaImmutableClass> superClasses;
  // do not do it volatile it is already SAFE TO USE IT in MT mode.
  private Collection<SchemaImmutableClass> subclasses;
  private boolean restricted;
  private boolean isVertexType;
  private boolean isEdgeType;
  private boolean triggered;
  private boolean function;
  private boolean scheduler;
  private boolean sequence;
  private boolean user;
  private boolean role;
  private boolean securityPolicy;
  private HashSet<Index> indexes;
  private final boolean isRemote;

  public SchemaImmutableClass(DatabaseSessionInternal session, final SchemaClassInternal oClass,
      final ImmutableSchema schema) {
    isAbstract = oClass.isAbstract(session);
    strictMode = oClass.isStrictMode(session);
    this.schema = schema;
    this.isRemote = session.isRemote();

    superClassesNames = oClass.getSuperClassesNames(session);
    superClasses = new ArrayList<>(superClassesNames.size());

    name = oClass.getName(session);
    streamAbleName = oClass.getStreamableName(session);
    clusterSelection = oClass.getClusterSelection(session);
    clusterIds = oClass.getClusterIds(session);
    polymorphicClusterIds = oClass.getPolymorphicClusterIds(session);

    baseClassesNames = new ArrayList<>();
    for (var baseClass : oClass.getSubclasses(session)) {
      baseClassesNames.add(baseClass.getName(session));
    }

    shortName = oClass.getShortName(session);

    properties = new HashMap<>();
    for (var p : oClass.declaredProperties(session)) {
      properties.put(p.getName(session),
          new ImmutableSchemaProperty(session, (SchemaPropertyInternal) p, this));
    }

    Map<String, String> customFields = new HashMap<String, String>();
    for (var key : oClass.getCustomKeys(session)) {
      customFields.put(key, oClass.getCustom(session, key));
    }

    this.customFields = Collections.unmodifiableMap(customFields);
    this.description = oClass.getDescription(session);
  }

  public void init(DatabaseSessionInternal session) {
    if (!inited) {
      initSuperClasses(session);

      final Collection<SchemaProperty> allProperties = new ArrayList<SchemaProperty>();
      final Map<String, SchemaProperty> allPropsMap = new HashMap<String, SchemaProperty>(20);
      for (var i = superClasses.size() - 1; i >= 0; i--) {
        allProperties.addAll(superClasses.get(i).allProperties);
        allPropsMap.putAll(superClasses.get(i).allPropertiesMap);
      }
      allProperties.addAll(properties.values());
      for (SchemaProperty p : properties.values()) {
        final var propName = p.getName(session);

        if (!allPropsMap.containsKey(propName)) {
          allPropsMap.put(propName, p);
        }
      }

      this.allProperties = Collections.unmodifiableCollection(allProperties);
      this.allPropertiesMap = Collections.unmodifiableMap(allPropsMap);
      this.restricted = isSubClassOf(session, SecurityShared.RESTRICTED_CLASSNAME);
      this.isVertexType = isSubClassOf(session, SchemaClass.VERTEX_CLASS_NAME);
      this.isEdgeType = isSubClassOf(session, SchemaClass.EDGE_CLASS_NAME);
      this.triggered = isSubClassOf(session, ClassTrigger.CLASSNAME);
      this.function = isSubClassOf(session, FunctionLibraryImpl.CLASSNAME);
      this.scheduler = isSubClassOf(session, ScheduledEvent.CLASS_NAME);
      this.sequence = isSubClassOf(session, DBSequence.CLASS_NAME);
      this.user = isSubClassOf(session, SecurityUserImpl.CLASS_NAME);
      this.role = isSubClassOf(session, Role.CLASS_NAME);
      this.securityPolicy = isSubClassOf(session, SecurityPolicy.CLASS_NAME);
      this.indexes = new HashSet<>();
      if (!isRemote) {
        getRawIndexes(session, indexes);
      }
    }

    inited = true;
  }

  public boolean isSecurityPolicy() {
    return securityPolicy;
  }

  @Override
  public boolean isAbstract(DatabaseSession session) {
    return isAbstract;
  }

  @Override
  public SchemaClass setAbstract(DatabaseSession session, boolean iAbstract) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isStrictMode(DatabaseSession session) {
    return strictMode;
  }

  @Override
  public void setStrictMode(DatabaseSession session, boolean iMode) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public SchemaClass getSuperClass(DatabaseSession session) {
    initSuperClasses((DatabaseSessionInternal) session);
    return superClasses.isEmpty() ? null : superClasses.getFirst();
  }

  @Override
  @Deprecated
  public SchemaClass setSuperClass(DatabaseSession session, SchemaClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SchemaClass> getSuperClasses(DatabaseSession session) {
    return Collections.unmodifiableList(superClasses);
  }

  @Override
  public boolean hasSuperClasses(DatabaseSession session) {
    return !superClasses.isEmpty();
  }

  @Override
  public List<String> getSuperClassesNames(DatabaseSession session) {
    return superClassesNames;
  }

  @Override
  public SchemaClass setSuperClasses(DatabaseSession session, List<? extends SchemaClass> classes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass addSuperClass(DatabaseSession session, SchemaClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeSuperClass(DatabaseSession session, SchemaClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName(DatabaseSession session) {
    return name;
  }

  @Override
  public SchemaClass setName(DatabaseSession session, String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStreamableName(DatabaseSession session) {
    return streamAbleName;
  }

  @Override
  public Collection<SchemaProperty> declaredProperties(DatabaseSession session) {
    return Collections.unmodifiableCollection(properties.values());
  }

  @Override
  public Collection<SchemaProperty> properties(DatabaseSession session) {
    return allProperties;
  }

  @Override
  public Map<String, SchemaProperty> propertiesMap(DatabaseSession session) {
    return allPropertiesMap;
  }

  public void getIndexedProperties(DatabaseSessionInternal session,
      Collection<SchemaProperty> indexedProperties) {
    for (SchemaProperty p : properties.values()) {
      if (areIndexed(session, p.getName(session))) {
        indexedProperties.add(p);
      }
    }
    initSuperClasses(session);
    for (var superClass : superClasses) {
      superClass.getIndexedProperties(session, indexedProperties);
    }
  }

  @Override
  public Collection<SchemaProperty> getIndexedProperties(DatabaseSession session) {
    Collection<SchemaProperty> indexedProps = new HashSet<SchemaProperty>();
    getIndexedProperties((DatabaseSessionInternal) session, indexedProps);
    return indexedProps;
  }

  @Override
  public SchemaProperty getProperty(DatabaseSession session, String propertyName) {
    return getPropertyInternal((DatabaseSessionInternal) session, propertyName);
  }

  @Override
  public SchemaPropertyInternal getPropertyInternal(DatabaseSessionInternal session,
      String propertyName) {
    initSuperClasses(session);

    var p = properties.get(propertyName);
    if (p != null) {
      return p;
    }

    for (var i = 0; i < superClasses.size() && p == null; i++) {
      p = superClasses.get(i).getPropertyInternal(session, propertyName);
    }

    return p;
  }

  @Override
  public SchemaProperty createProperty(DatabaseSession session, String iPropertyName,
      PropertyType iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaProperty createProperty(DatabaseSession session, String iPropertyName,
      PropertyType iType,
      SchemaClass iLinkedClass) {
    throw new UnsupportedOperationException();
  }


  @Override
  public SchemaProperty createProperty(DatabaseSession session, String iPropertyName,
      PropertyType iType,
      PropertyType iLinkedType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropProperty(DatabaseSession session, String iPropertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsProperty(DatabaseSession session, String propertyName) {
    var result = properties.containsKey(propertyName);
    if (result) {
      return true;
    }
    for (var superClass : superClasses) {
      result = superClass.existsProperty(session, propertyName);

      if (result) {
        return true;
      }
    }

    return false;
  }

  @Override
  public int getClusterForNewInstance(DatabaseSessionInternal session, final EntityImpl entity) {
    return clusterSelection.getCluster(session, this, entity);
  }


  @Override
  public int[] getClusterIds(DatabaseSession session) {
    return clusterIds;
  }

  @Override
  public SchemaClass addClusterId(DatabaseSession session, int iId) {
    throw new UnsupportedOperationException();
  }

  public ClusterSelectionStrategy getClusterSelection(DatabaseSessionInternal session) {
    return clusterSelection;
  }


  @Override
  public SchemaClass setClusterSelection(DatabaseSession session, String iStrategyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass setClusterSelection(DatabaseSessionInternal session,
      ClusterSelectionStrategy clusterSelection) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass addCluster(DatabaseSession session, String iClusterName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass truncateCluster(DatabaseSessionInternal session, String clusterName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass removeClusterId(DatabaseSession session, int iId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getPolymorphicClusterIds(DatabaseSession session) {
    return Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length);
  }

  public ImmutableSchema getSchema() {
    return schema;
  }

  @Override
  public Collection<SchemaClass> getSubclasses(DatabaseSession session) {
    initBaseClasses();
    return new ArrayList<SchemaClass>(subclasses);
  }

  @Override
  public Collection<SchemaClass> getAllSubclasses(DatabaseSession session) {
    initBaseClasses();

    final Set<SchemaClass> set = new HashSet<SchemaClass>(getSubclasses(session));

    for (var c : subclasses) {
      set.addAll(c.getAllSubclasses(session));
    }

    return set;
  }

  @Override
  public Collection<SchemaClass> getAllSuperClasses() {
    Set<SchemaClass> ret = new HashSet<SchemaClass>();
    getAllSuperClasses(ret);
    return ret;
  }

  private void getAllSuperClasses(Set<SchemaClass> set) {
    set.addAll(superClasses);
    for (var superClass : superClasses) {
      superClass.getAllSuperClasses(set);
    }
  }


  public long count(DatabaseSessionInternal session) {
    return count(session, true);
  }

  public long count(DatabaseSessionInternal session, boolean isPolymorphic) {
    return session.countClass(name, isPolymorphic);
  }

  public long countImpl(boolean isPolymorphic, DatabaseSessionInternal db) {
    if (isPolymorphic) {
      return db
          .countClusterElements(
              SchemaClassImpl.readableClusters(db, polymorphicClusterIds, name));
    }

    return db
        .countClusterElements(SchemaClassImpl.readableClusters(db, clusterIds, name));
  }

  @Override
  public void truncate(DatabaseSessionInternal session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSubClassOf(DatabaseSession session, final String iClassName) {
    if (iClassName == null) {
      return false;
    }

    if (iClassName.equalsIgnoreCase(name) || iClassName.equalsIgnoreCase(shortName)) {
      return true;
    }

    final var s = superClasses.size();
    for (var superClass : superClasses) {
      if (superClass.isSubClassOf(session, iClassName)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean isSubClassOf(DatabaseSession session, final SchemaClass clazz) {
    if (clazz == null) {
      return false;
    }
    if (equals(clazz)) {
      return true;
    }

    for (var superClass : superClasses) {
      if (superClass.isSubClassOf(session, clazz)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isSuperClassOf(DatabaseSession session, SchemaClass clazz) {
    return clazz != null && clazz.isSubClassOf(session, this);
  }

  @Override
  public String getShortName(DatabaseSession session) {
    return shortName;
  }

  @Override
  public SchemaClass setShortName(DatabaseSession session, String shortName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDescription(DatabaseSession session) {
    return description;
  }

  @Override
  public SchemaClass setDescription(DatabaseSession session, String iDescription) {
    throw new UnsupportedOperationException();
  }


  public Object get(DatabaseSessionInternal db, ATTRIBUTES iAttribute) {
    if (iAttribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    return switch (iAttribute) {
      case NAME -> name;
      case SHORTNAME -> shortName;
      case SUPERCLASS -> getSuperClass(db);
      case SUPERCLASSES -> getSuperClasses(db);
      case STRICT_MODE -> strictMode;
      case ABSTRACT -> isAbstract;
      case CLUSTER_SELECTION -> clusterSelection;
      case CUSTOM -> customFields;
      case DESCRIPTION -> description;
      default -> throw new IllegalArgumentException("Cannot find attribute '" + iAttribute + "'");
    };

  }

  @Override
  public void createIndex(DatabaseSession session, String iName, INDEX_TYPE iType,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createIndex(DatabaseSession session, String iName, String iType,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createIndex(
      DatabaseSession session, String iName, INDEX_TYPE iType,
      ProgressListener iProgressListener,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getInvolvedIndexes(DatabaseSessionInternal session,
      Collection<String> fields) {
    initSuperClasses(session);

    final Set<String> result = new HashSet<>(getClassInvolvedIndexes(session, fields));

    for (var superClass : superClasses) {
      result.addAll(superClass.getInvolvedIndexes(session, fields));
    }
    return result;
  }

  @Override
  public Set<Index> getInvolvedIndexesInternal(DatabaseSessionInternal session,
      Collection<String> fields) {
    initSuperClasses(session);

    final Set<Index> result = new HashSet<>(getClassInvolvedIndexesInternal(session, fields));
    for (var superClass : superClasses) {
      result.addAll(superClass.getInvolvedIndexesInternal(session, fields));
    }

    return result;
  }

  @Override
  public Set<String> getInvolvedIndexes(DatabaseSessionInternal session, String... fields) {
    return getInvolvedIndexes(session, Arrays.asList(fields));
  }

  @Override
  public Set<Index> getInvolvedIndexesInternal(DatabaseSessionInternal session, String... fields) {
    return getInvolvedIndexesInternal(session, Arrays.asList(fields));
  }

  @Override
  public Set<String> getClassInvolvedIndexes(DatabaseSessionInternal session,
      Collection<String> fields) {
    return getClassInvolvedIndexesInternal(session, fields).stream().map(Index::getName)
        .collect(HashSet::new, HashSet::add, HashSet::addAll);
  }

  @Override
  public Set<Index> getClassInvolvedIndexesInternal(DatabaseSessionInternal session,
      Collection<String> fields) {
    var sessionInternal = session;
    final var indexManager = sessionInternal.getMetadata().getIndexManagerInternal();
    return indexManager.getClassInvolvedIndexes(sessionInternal, name, fields);
  }

  @Override
  public Set<String> getClassInvolvedIndexes(DatabaseSessionInternal session, String... fields) {
    return getClassInvolvedIndexes(session, Arrays.asList(fields));
  }

  @Override
  public Set<Index> getClassInvolvedIndexesInternal(DatabaseSessionInternal session,
      String... fields) {
    return getClassInvolvedIndexesInternal(session, Arrays.asList(fields));
  }

  @Override
  public boolean areIndexed(DatabaseSessionInternal session, Collection<String> fields) {
    final var database = session;
    final var indexManager = database.getMetadata().getIndexManagerInternal();
    final var currentClassResult = indexManager.areIndexed(session, name, fields);

    initSuperClasses(database);

    if (currentClassResult) {
      return true;
    }
    for (var superClass : superClasses) {
      if (superClass.areIndexed(session, fields)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean areIndexed(DatabaseSessionInternal session, String... fields) {
    return areIndexed(session, Arrays.asList(fields));
  }

  @Override
  public Set<String> getClassIndexes(DatabaseSessionInternal session) {
    if (isRemote) {
      throw new UnsupportedOperationException("Not supported for remote environment.");
    }

    return this.indexes.stream().map(Index::getName).collect(HashSet::new, HashSet::add,
        HashSet::addAll);
  }

  @Override
  public String getClusterSelectionStrategyName(DatabaseSession session) {
    return clusterSelection.getName();
  }

  @Override
  public SchemaProperty createProperty(DatabaseSessionInternal session, String iPropertyName,
      PropertyType iType, PropertyType iLinkedType, boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaProperty createProperty(DatabaseSessionInternal session, String iPropertyName,
      PropertyType iType, SchemaClass iLinkedClass, boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createIndex(DatabaseSession session, String iName, String iType,
      ProgressListener iProgressListener, Map<String, ?> metadata, String algorithm,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createIndex(DatabaseSession session, String iName, String iType,
      ProgressListener iProgressListener, Map<String, ?> metadata, String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Index> getClassIndexesInternal(DatabaseSessionInternal session) {
    if (isRemote) {
      throw new UnsupportedOperationException("Not supported for remote environment.");
    }
    return this.indexes;
  }

  @Override
  public Index getClassIndex(DatabaseSessionInternal session, String name) {
    return session
        .getMetadata()
        .getIndexManagerInternal()
        .getClassIndex(session, this.name, name);
  }

  public void getClassIndexes(DatabaseSession session, final Collection<Index> indexes) {
    final var database = (DatabaseSessionInternal) session;
    database.getMetadata().getIndexManagerInternal().getClassIndexes(database, name, indexes);
  }

  public void getRawClassIndexes(DatabaseSessionInternal session, final Collection<Index> indexes) {
    session.getMetadata().getIndexManagerInternal().getClassRawIndexes(session, name, indexes);
  }

  @Override
  public void getIndexesInternal(DatabaseSessionInternal session, final Collection<Index> indexes) {
    initSuperClasses(session);

    getClassIndexes(session, indexes);
    for (SchemaClassInternal superClass : superClasses) {
      superClass.getIndexesInternal(session, indexes);
    }
  }

  public void getRawIndexes(DatabaseSessionInternal db, final Collection<Index> indexes) {
    initSuperClasses(db);

    getRawClassIndexes(db, indexes);
    for (var superClass : superClasses) {
      superClass.getRawIndexes(db, indexes);
    }
  }

  @Override
  public Set<String> getIndexes(DatabaseSessionInternal session) {
    if (isRemote) {
      throw new UnsupportedOperationException("Not supported for remote environment.");
    }
    return this.indexes.stream().map(Index::getName).collect(HashSet::new, HashSet::add,
        HashSet::addAll);
  }

  @Override
  public Set<Index> getIndexesInternal(DatabaseSessionInternal session) {
    if (isRemote) {
      throw new UnsupportedOperationException("Not supported for remote environment.");
    }
    return this.indexes;
  }

  public Set<Index> getRawIndexes() {
    if (isRemote) {
      throw new UnsupportedOperationException("Not supported for remote environment.");
    }

    return indexes;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public String getCustom(DatabaseSession session, final String iName) {
    return customFields.get(iName);
  }

  @Override
  public SchemaClass setCustom(DatabaseSession session, String iName, String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeCustom(DatabaseSession session, String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearCustom(DatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getCustomKeys(DatabaseSession session) {
    return Collections.unmodifiableSet(customFields.keySet());
  }

  @Override
  public boolean hasClusterId(final int clusterId) {
    return Arrays.binarySearch(clusterIds, clusterId) >= 0;
  }

  @Override
  public boolean hasPolymorphicClusterId(final int clusterId) {
    return Arrays.binarySearch(polymorphicClusterIds, clusterId) >= 0;
  }


  @Override
  public SchemaClass set(DatabaseSessionInternal session, ATTRIBUTES attribute, Object value) {
    throw new UnsupportedOperationException();
  }

  private void initSuperClasses(DatabaseSessionInternal db) {
    if (superClassesNames != null && superClassesNames.size() != superClasses.size()) {
      superClasses.clear();
      for (var superClassName : superClassesNames) {
        var superClass = (SchemaImmutableClass) schema.getClass(superClassName);
        superClass.init(db);
        superClasses.add(superClass);
      }
    }
  }

  private void initBaseClasses() {
    if (subclasses == null) {
      final List<SchemaImmutableClass> result = new ArrayList<SchemaImmutableClass>(
          baseClassesNames.size());
      for (var clsName : baseClassesNames) {
        result.add((SchemaImmutableClass) schema.getClass(clsName));
      }

      subclasses = result;
    }
  }

  public boolean isRestricted() {
    return restricted;
  }

  public boolean isEdgeType(DatabaseSession session) {
    return isEdgeType;
  }

  public boolean isVertexType(DatabaseSession session) {
    return isVertexType;
  }

  public boolean isTriggered() {
    return triggered;
  }

  public boolean isFunction() {
    return function;
  }

  public boolean isScheduler() {
    return scheduler;
  }

  public boolean isUser() {
    return user;
  }

  public boolean isRole() {
    return role;
  }

  public boolean isSequence() {
    return sequence;
  }

  public boolean isSystemClass() {
    return restricted || function || scheduler || user || role || sequence || securityPolicy;
  }
}
