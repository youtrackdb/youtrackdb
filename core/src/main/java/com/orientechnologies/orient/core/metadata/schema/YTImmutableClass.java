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
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicy;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.sequence.YTSequence;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @since 10/21/14
 */
public class YTImmutableClass implements YTClass {

  /**
   * use YTClass.EDGE_CLASS_NAME instead
   */
  @Deprecated
  public static final String EDGE_CLASS_NAME = YTClass.EDGE_CLASS_NAME;

  /**
   * use YTClass.EDGE_CLASS_NAME instead
   */
  @Deprecated
  public static final String VERTEX_CLASS_NAME = YTClass.VERTEX_CLASS_NAME;

  private boolean inited = false;
  private final boolean isAbstract;
  private final boolean strictMode;
  private final String name;
  private final String streamAbleName;
  private final Map<String, YTProperty> properties;
  private Map<String, YTProperty> allPropertiesMap;
  private Collection<YTProperty> allProperties;
  private final OClusterSelectionStrategy clusterSelection;
  private final int defaultClusterId;
  private final int[] clusterIds;
  private final int[] polymorphicClusterIds;
  private final Collection<String> baseClassesNames;
  private final List<String> superClassesNames;
  private final float overSize;
  private final float classOverSize;
  private final String shortName;
  private final Map<String, String> customFields;
  private final String description;

  private final YTImmutableSchema schema;
  // do not do it volatile it is already SAFE TO USE IT in MT mode.
  private final List<YTImmutableClass> superClasses;
  // do not do it volatile it is already SAFE TO USE IT in MT mode.
  private Collection<YTImmutableClass> subclasses;
  private boolean restricted;
  private boolean isVertexType;
  private boolean isEdgeType;
  private boolean triggered;
  private boolean function;
  private boolean scheduler;
  private boolean sequence;
  private boolean ouser;
  private boolean orole;
  private boolean securityPolicy;
  private OIndex autoShardingIndex;
  private HashSet<OIndex> indexes;

  public YTImmutableClass(YTDatabaseSessionInternal session, final YTClass oClass,
      final YTImmutableSchema schema) {
    isAbstract = oClass.isAbstract();
    strictMode = oClass.isStrictMode();
    this.schema = schema;

    superClassesNames = oClass.getSuperClassesNames();
    superClasses = new ArrayList<YTImmutableClass>(superClassesNames.size());

    name = oClass.getName();
    streamAbleName = oClass.getStreamableName();
    clusterSelection = oClass.getClusterSelection();
    defaultClusterId = oClass.getDefaultClusterId();
    clusterIds = oClass.getClusterIds();
    polymorphicClusterIds = oClass.getPolymorphicClusterIds();

    baseClassesNames = new ArrayList<String>();
    for (YTClass baseClass : oClass.getSubclasses()) {
      baseClassesNames.add(baseClass.getName());
    }

    overSize = oClass.getOverSize();
    classOverSize = oClass.getClassOverSize();
    shortName = oClass.getShortName();

    properties = new HashMap<String, YTProperty>();
    for (YTProperty p : oClass.declaredProperties()) {
      properties.put(p.getName(), new YTImmutableProperty(session, p, this));
    }

    Map<String, String> customFields = new HashMap<String, String>();
    for (String key : oClass.getCustomKeys()) {
      customFields.put(key, oClass.getCustom(key));
    }

    this.customFields = Collections.unmodifiableMap(customFields);
    this.description = oClass.getDescription();
  }

  public void init() {
    if (!inited) {
      initSuperClasses();

      final Collection<YTProperty> allProperties = new ArrayList<YTProperty>();
      final Map<String, YTProperty> allPropsMap = new HashMap<String, YTProperty>(20);
      for (int i = superClasses.size() - 1; i >= 0; i--) {
        allProperties.addAll(superClasses.get(i).allProperties);
        allPropsMap.putAll(superClasses.get(i).allPropertiesMap);
      }
      allProperties.addAll(properties.values());
      for (YTProperty p : properties.values()) {
        final String propName = p.getName();

        if (!allPropsMap.containsKey(propName)) {
          allPropsMap.put(propName, p);
        }
      }

      this.allProperties = Collections.unmodifiableCollection(allProperties);
      this.allPropertiesMap = Collections.unmodifiableMap(allPropsMap);
      this.restricted = isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME);
      this.isVertexType = isSubClassOf(YTClass.VERTEX_CLASS_NAME);
      this.isEdgeType = isSubClassOf(YTClass.EDGE_CLASS_NAME);
      this.triggered = isSubClassOf(OClassTrigger.CLASSNAME);
      this.function = isSubClassOf(OFunctionLibraryImpl.CLASSNAME);
      this.scheduler = isSubClassOf(OScheduledEvent.CLASS_NAME);
      this.sequence = isSubClassOf(YTSequence.CLASS_NAME);
      this.ouser = isSubClassOf(OUser.CLASS_NAME);
      this.orole = isSubClassOf(ORole.CLASS_NAME);
      this.securityPolicy = OSecurityPolicy.class.getSimpleName().equals(this.name);
      this.indexes = new HashSet<>();
      getRawIndexes(indexes);

      final YTDatabaseSessionInternal db = getDatabase();
      if (db != null
          && db.getMetadata() != null
          && db.getMetadata().getIndexManagerInternal() != null) {
        this.autoShardingIndex =
            db.getMetadata().getIndexManagerInternal().getClassAutoShardingIndex(db, name);
      } else {
        this.autoShardingIndex = null;
      }
    }

    inited = true;
  }

  public boolean isSecurityPolicy() {
    return securityPolicy;
  }

  @Override
  public boolean isAbstract() {
    return isAbstract;
  }

  @Override
  public YTClass setAbstract(YTDatabaseSession session, boolean iAbstract) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isStrictMode() {
    return strictMode;
  }

  @Override
  public YTClass setStrictMode(YTDatabaseSession session, boolean iMode) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public YTClass getSuperClass() {
    initSuperClasses();

    return superClasses.isEmpty() ? null : superClasses.get(0);
  }

  @Override
  @Deprecated
  public YTClass setSuperClass(YTDatabaseSession session, YTClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<YTClass> getSuperClasses() {
    return Collections.unmodifiableList(superClasses);
  }

  @Override
  public boolean hasSuperClasses() {
    return !superClasses.isEmpty();
  }

  @Override
  public List<String> getSuperClassesNames() {
    return superClassesNames;
  }

  @Override
  public YTClass setSuperClasses(YTDatabaseSession session, List<? extends YTClass> classes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass addSuperClass(YTDatabaseSession session, YTClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass removeSuperClass(YTDatabaseSession session, YTClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public YTClass setName(YTDatabaseSession session, String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStreamableName() {
    return streamAbleName;
  }

  @Override
  public Collection<YTProperty> declaredProperties() {
    return Collections.unmodifiableCollection(properties.values());
  }

  @Override
  public Collection<YTProperty> properties(YTDatabaseSession session) {
    return allProperties;
  }

  @Override
  public Map<String, YTProperty> propertiesMap(YTDatabaseSession session) {
    return allPropertiesMap;
  }

  public void getIndexedProperties(YTDatabaseSessionInternal session,
      Collection<YTProperty> indexedProperties) {
    for (YTProperty p : properties.values()) {
      if (areIndexed(session, p.getName())) {
        indexedProperties.add(p);
      }
    }
    initSuperClasses();
    for (YTImmutableClass superClass : superClasses) {
      superClass.getIndexedProperties(session, indexedProperties);
    }
  }

  @Override
  public Collection<YTProperty> getIndexedProperties(YTDatabaseSession session) {
    Collection<YTProperty> indexedProps = new HashSet<YTProperty>();
    getIndexedProperties((YTDatabaseSessionInternal) session, indexedProps);
    return indexedProps;
  }

  @Override
  public YTProperty getProperty(String propertyName) {
    initSuperClasses();

    YTProperty p = properties.get(propertyName);
    if (p != null) {
      return p;
    }
    for (int i = 0; i < superClasses.size() && p == null; i++) {
      p = superClasses.get(i).getProperty(propertyName);
    }
    return p;
  }

  @Override
  public YTProperty createProperty(YTDatabaseSession session, String iPropertyName, YTType iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTProperty createProperty(YTDatabaseSession session, String iPropertyName, YTType iType,
      YTClass iLinkedClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTProperty createProperty(
      YTDatabaseSession session, String iPropertyName, YTType iType, YTClass iLinkedClass,
      boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTProperty createProperty(YTDatabaseSession session, String iPropertyName, YTType iType,
      YTType iLinkedType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTProperty createProperty(
      YTDatabaseSession session, String iPropertyName, YTType iType, YTType iLinkedType,
      boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropProperty(YTDatabaseSession session, String iPropertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsProperty(String propertyName) {
    boolean result = properties.containsKey(propertyName);
    if (result) {
      return true;
    }
    for (YTImmutableClass superClass : superClasses) {
      result = superClass.existsProperty(propertyName);
      if (result) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int getClusterForNewInstance(final YTDocument doc) {
    return clusterSelection.getCluster(this, doc);
  }

  @Override
  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  @Override
  public void setDefaultClusterId(YTDatabaseSession session, int iDefaultClusterId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getClusterIds() {
    return clusterIds;
  }

  @Override
  public YTClass addClusterId(YTDatabaseSession session, int iId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClusterSelectionStrategy getClusterSelection() {
    return clusterSelection;
  }

  @Override
  public YTClass setClusterSelection(YTDatabaseSession session,
      OClusterSelectionStrategy clusterSelection) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass setClusterSelection(YTDatabaseSession session, String iStrategyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass addCluster(YTDatabaseSession session, String iClusterName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass truncateCluster(YTDatabaseSession session, String clusterName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass removeClusterId(YTDatabaseSession session, int iId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] getPolymorphicClusterIds() {
    return Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length);
  }

  public YTImmutableSchema getSchema() {
    return schema;
  }

  @Override
  public Collection<YTClass> getSubclasses() {
    initBaseClasses();

    ArrayList<YTClass> result = new ArrayList<YTClass>();
    for (YTClass c : subclasses) {
      result.add(c);
    }

    return result;
  }

  @Override
  public Collection<YTClass> getAllSubclasses() {
    initBaseClasses();

    final Set<YTClass> set = new HashSet<YTClass>();
    set.addAll(getSubclasses());

    for (YTImmutableClass c : subclasses) {
      set.addAll(c.getAllSubclasses());
    }

    return set;
  }

  @Override
  @Deprecated
  public Collection<YTClass> getBaseClasses() {
    return getSubclasses();
  }

  @Override
  @Deprecated
  public Collection<YTClass> getAllBaseClasses() {
    return getAllSubclasses();
  }

  @Override
  public Collection<YTClass> getAllSuperClasses() {
    Set<YTClass> ret = new HashSet<YTClass>();
    getAllSuperClasses(ret);
    return ret;
  }

  private void getAllSuperClasses(Set<YTClass> set) {
    set.addAll(superClasses);
    for (YTImmutableClass superClass : superClasses) {
      superClass.getAllSuperClasses(set);
    }
  }

  @Override
  public long getSize(YTDatabaseSessionInternal session) {
    long size = 0;
    for (int clusterId : clusterIds) {
      size += getDatabase().getClusterRecordSizeById(clusterId);
    }

    return size;
  }

  @Override
  public float getOverSize() {
    return overSize;
  }

  @Override
  public float getClassOverSize() {
    return classOverSize;
  }

  @Override
  public YTClass setOverSize(YTDatabaseSession session, float overSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long count(YTDatabaseSession session) {
    return count(session, true);
  }

  @Override
  public long count(YTDatabaseSession session, boolean isPolymorphic) {
    return getDatabase().countClass(name, isPolymorphic);
  }

  public long countImpl(boolean isPolymorphic) {
    if (isPolymorphic) {
      return getDatabase()
          .countClusterElements(
              YTClassImpl.readableClusters(getDatabase(), polymorphicClusterIds, name));
    }

    return getDatabase()
        .countClusterElements(YTClassImpl.readableClusters(getDatabase(), clusterIds, name));
  }

  @Override
  public void truncate(YTDatabaseSession session) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSubClassOf(final String iClassName) {
    if (iClassName == null) {
      return false;
    }

    if (iClassName.equalsIgnoreCase(name) || iClassName.equalsIgnoreCase(shortName)) {
      return true;
    }

    final int s = superClasses.size();
    for (int i = 0; i < s; ++i) {
      if (superClasses.get(i).isSubClassOf(iClassName)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean isSubClassOf(final YTClass clazz) {
    if (clazz == null) {
      return false;
    }
    if (equals(clazz)) {
      return true;
    }

    final int s = superClasses.size();
    for (int i = 0; i < s; ++i) {
      if (superClasses.get(i).isSubClassOf(clazz)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isSuperClassOf(YTClass clazz) {
    return clazz != null && clazz.isSubClassOf(this);
  }

  @Override
  public String getShortName() {
    return shortName;
  }

  @Override
  public YTClass setShortName(YTDatabaseSession session, String shortName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public YTClass setDescription(YTDatabaseSession session, String iDescription) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(ATTRIBUTES iAttribute) {
    if (iAttribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    switch (iAttribute) {
      case NAME:
        return name;
      case SHORTNAME:
        return shortName;
      case SUPERCLASS:
        return getSuperClass();
      case SUPERCLASSES:
        return getSuperClasses();
      case OVERSIZE:
        return overSize;
      case STRICTMODE:
        return strictMode;
      case ABSTRACT:
        return isAbstract;
      case CLUSTERSELECTION:
        return clusterSelection;
      case CUSTOM:
        return customFields;
      case DESCRIPTION:
        return description;
    }

    throw new IllegalArgumentException("Cannot find attribute '" + iAttribute + "'");
  }

  @Override
  public YTClass set(YTDatabaseSession session, ATTRIBUTES attribute, Object iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, String iName, INDEX_TYPE iType,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, String iName, String iType,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(
      YTDatabaseSession session, String iName, INDEX_TYPE iType,
      OProgressListener iProgressListener,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(
      YTDatabaseSession session, String iName,
      String iType,
      OProgressListener iProgressListener,
      YTDocument metadata,
      String algorithm,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(
      YTDatabaseSession session, String iName,
      String iType,
      OProgressListener iProgressListener,
      YTDocument metadata,
      String... fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<OIndex> getInvolvedIndexes(YTDatabaseSession session, Collection<String> fields) {
    initSuperClasses();

    final Set<OIndex> result = new HashSet<OIndex>(getClassInvolvedIndexes(session, fields));

    for (YTImmutableClass superClass : superClasses) {
      result.addAll(superClass.getInvolvedIndexes(session, fields));
    }
    return result;
  }

  @Override
  public Set<OIndex> getInvolvedIndexes(YTDatabaseSession session, String... fields) {
    return getInvolvedIndexes(session, Arrays.asList(fields));
  }

  @Override
  public Set<OIndex> getClassInvolvedIndexes(YTDatabaseSession session, Collection<String> fields) {
    final YTDatabaseSessionInternal database = getDatabase();
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    return indexManager.getClassInvolvedIndexes(database, name, fields);
  }

  @Override
  public Set<OIndex> getClassInvolvedIndexes(YTDatabaseSession session, String... fields) {
    return getClassInvolvedIndexes(session, Arrays.asList(fields));
  }

  @Override
  public boolean areIndexed(YTDatabaseSession session, Collection<String> fields) {
    final YTDatabaseSessionInternal database = getDatabase();
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    final boolean currentClassResult = indexManager.areIndexed(name, fields);

    initSuperClasses();

    if (currentClassResult) {
      return true;
    }
    for (YTImmutableClass superClass : superClasses) {
      if (superClass.areIndexed(session, fields)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean areIndexed(YTDatabaseSession session, String... fields) {
    return areIndexed(session, Arrays.asList(fields));
  }

  @Override
  public OIndex getClassIndex(YTDatabaseSession session, String iName) {
    final YTDatabaseSessionInternal database = getDatabase();
    return database
        .getMetadata()
        .getIndexManagerInternal()
        .getClassIndex(database, this.name, iName);
  }

  @Override
  public Set<OIndex> getClassIndexes(YTDatabaseSession session) {
    return this.indexes;
  }

  @Override
  public void getClassIndexes(YTDatabaseSession session, final Collection<OIndex> indexes) {
    final YTDatabaseSessionInternal database = getDatabase();
    database.getMetadata().getIndexManagerInternal().getClassIndexes(database, name, indexes);
  }

  public void getRawClassIndexes(final Collection<OIndex> indexes) {
    getDatabase().getMetadata().getIndexManagerInternal().getClassRawIndexes(name, indexes);
  }

  @Override
  public void getIndexes(YTDatabaseSession session, final Collection<OIndex> indexes) {
    initSuperClasses();

    getClassIndexes(session, indexes);
    for (YTClass superClass : superClasses) {
      superClass.getIndexes(session, indexes);
    }
  }

  public void getRawIndexes(final Collection<OIndex> indexes) {
    initSuperClasses();

    getRawClassIndexes(indexes);
    for (YTImmutableClass superClass : superClasses) {
      superClass.getRawIndexes(indexes);
    }
  }

  @Override
  public Set<OIndex> getIndexes(YTDatabaseSession session) {
    return this.indexes;
  }

  public Set<OIndex> getRawIndexes() {
    return indexes;
  }

  @Override
  public OIndex getAutoShardingIndex(YTDatabaseSession session) {
    return autoShardingIndex;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result;
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!YTClass.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    final YTClass other = (YTClass) obj;
    if (name == null) {
      return other.getName() == null;
    } else {
      return name.equals(other.getName());
    }
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public String getCustom(final String iName) {
    return customFields.get(iName);
  }

  @Override
  public YTClass setCustom(YTDatabaseSession session, String iName, String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeCustom(YTDatabaseSession session, String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearCustom(YTDatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getCustomKeys() {
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
  public int compareTo(final YTClass other) {
    return name.compareTo(other.getName());
  }

  protected YTDatabaseSessionInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  private Map<String, String> getCustomInternal() {
    return customFields;
  }

  private void initSuperClasses() {
    if (superClassesNames != null && superClassesNames.size() != superClasses.size()) {
      superClasses.clear();
      for (String superClassName : superClassesNames) {
        YTImmutableClass superClass = (YTImmutableClass) schema.getClass(superClassName);
        superClass.init();
        superClasses.add(superClass);
      }
    }
  }

  private void initBaseClasses() {
    if (subclasses == null) {
      final List<YTImmutableClass> result = new ArrayList<YTImmutableClass>(
          baseClassesNames.size());
      for (String clsName : baseClassesNames) {
        result.add((YTImmutableClass) schema.getClass(clsName));
      }

      subclasses = result;
    }
  }

  public boolean isRestricted() {
    return restricted;
  }

  public boolean isEdgeType() {
    return isEdgeType;
  }

  public boolean isVertexType() {
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

  public boolean isOuser() {
    return ouser;
  }

  public boolean isOrole() {
    return orole;
  }

  public boolean isSequence() {
    return sequence;
  }
}
