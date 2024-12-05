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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.exception.YTSchemaException;
import com.orientechnologies.orient.core.exception.YTSecurityAccessException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionFactory;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.YTIndexException;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sharding.auto.OAutoShardingClusterSelectionStrategy;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Schema Class implementation.
 */
@SuppressWarnings("unchecked")
public abstract class YTClassImpl implements YTClass {

  protected static final int NOT_EXISTENT_CLUSTER_ID = -1;
  protected final OSchemaShared owner;
  protected final Map<String, YTProperty> properties = new HashMap<String, YTProperty>();
  protected int defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
  protected String name;
  protected String description;
  protected int[] clusterIds;
  protected List<YTClassImpl> superClasses = new ArrayList<YTClassImpl>();
  protected int[] polymorphicClusterIds;
  protected List<YTClass> subclasses;
  protected float overSize = 0f;
  protected String shortName;
  protected boolean strictMode = false; // @SINCE v1.0rc8
  protected boolean abstractClass = false; // @SINCE v1.2.0
  protected Map<String, String> customFields;
  protected volatile OClusterSelectionStrategy clusterSelection; // @SINCE 1.7
  protected volatile int hashCode;

  private static final Set<String> reserved = new HashSet<String>();

  static {
    // reserved.add("select");
    reserved.add("traverse");
    reserved.add("insert");
    reserved.add("update");
    reserved.add("delete");
    reserved.add("from");
    reserved.add("where");
    reserved.add("skip");
    reserved.add("limit");
    reserved.add("timeout");
  }

  protected YTClassImpl(final OSchemaShared iOwner, final String iName, final int[] iClusterIds) {
    this(iOwner, iName);
    setClusterIds(iClusterIds);
    defaultClusterId = iClusterIds[0];
    if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID) {
      abstractClass = true;
    }

    if (abstractClass) {
      setPolymorphicClusterIds(OCommonConst.EMPTY_INT_ARRAY);
    } else {
      setPolymorphicClusterIds(iClusterIds);
    }

    clusterSelection = owner.getClusterSelectionFactory().newInstanceOfDefaultClass();
  }

  /**
   * Constructor used in unmarshalling.
   */
  protected YTClassImpl(final OSchemaShared iOwner, final String iName) {
    name = iName;
    owner = iOwner;
  }

  public static int[] readableClusters(
      final YTDatabaseSessionInternal db, final int[] iClusterIds, String className) {
    IntArrayList listOfReadableIds = new IntArrayList();

    boolean all = true;
    for (int clusterId : iClusterIds) {
      try {
        // This will exclude (filter out) any specific classes without explicit read permission.
        if (className != null) {
          db.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, className);
        }

        final String clusterName = db.getClusterNameById(clusterId);
        db.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, clusterName);
        listOfReadableIds.add(clusterId);
      } catch (YTSecurityAccessException ignore) {
        all = false;
        // if the cluster is inaccessible it's simply not processed in the list.add
      }
    }

    // JUST RETURN INPUT ARRAY (FASTER)
    if (all) {
      return iClusterIds;
    }

    final int[] readableClusterIds = new int[listOfReadableIds.size()];
    int index = 0;
    for (int i = 0; i < listOfReadableIds.size(); i++) {
      readableClusterIds[index++] = listOfReadableIds.getInt(i);
    }

    return readableClusterIds;
  }

  @Override
  public OClusterSelectionStrategy getClusterSelection() {
    acquireSchemaReadLock();
    try {
      return clusterSelection;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public YTClass setClusterSelection(YTDatabaseSession session,
      final OClusterSelectionStrategy clusterSelection) {
    return setClusterSelection(session, clusterSelection.getName());
  }

  public String getCustom(final String iName) {
    acquireSchemaReadLock();
    try {
      if (customFields == null) {
        return null;
      }

      return customFields.get(iName);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Map<String, String> getCustomInternal() {
    acquireSchemaReadLock();
    try {
      if (customFields != null) {
        return Collections.unmodifiableMap(customFields);
      }
      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void removeCustom(YTDatabaseSession session, final String name) {
    setCustom(session, name, null);
  }

  public Set<String> getCustomKeys() {
    acquireSchemaReadLock();
    try {
      if (customFields != null) {
        return Collections.unmodifiableSet(customFields.keySet());
      }
      return new HashSet<String>();
    } finally {
      releaseSchemaReadLock();
    }
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
  @Deprecated
  public YTClass getSuperClass() {
    acquireSchemaReadLock();
    try {
      return superClasses.isEmpty() ? null : superClasses.get(0);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  @Deprecated
  public YTClass setSuperClass(YTDatabaseSession session, YTClass iSuperClass) {
    setSuperClasses(session, iSuperClass != null ? List.of(iSuperClass) : Collections.EMPTY_LIST);
    return this;
  }

  public String getName() {
    acquireSchemaReadLock();
    try {
      return name;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public List<YTClass> getSuperClasses() {
    acquireSchemaReadLock();
    try {
      return Collections.unmodifiableList(superClasses);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public boolean hasSuperClasses() {
    acquireSchemaReadLock();
    try {
      return !superClasses.isEmpty();
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public List<String> getSuperClassesNames() {
    acquireSchemaReadLock();
    try {
      List<String> superClassesNames = new ArrayList<String>(superClasses.size());
      for (YTClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      return superClassesNames;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void setSuperClassesByNames(YTDatabaseSessionInternal session, List<String> classNames) {
    if (classNames == null) {
      classNames = Collections.EMPTY_LIST;
    }

    final List<YTClass> classes = new ArrayList<YTClass>(classNames.size());
    final YTSchema schema = session.getMetadata().getSchema();
    for (String className : classNames) {
      classes.add(schema.getClass(decodeClassName(className)));
    }
    setSuperClasses(session, classes);
  }

  protected abstract void setSuperClassesInternal(YTDatabaseSessionInternal session,
      final List<? extends YTClass> classes);

  public long getSize(YTDatabaseSessionInternal session) {
    acquireSchemaReadLock();
    try {
      long size = 0;
      for (int clusterId : clusterIds) {
        size += session.getClusterRecordSizeById(clusterId);
      }

      return size;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getShortName() {
    acquireSchemaReadLock();
    try {
      return shortName;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getDescription() {
    acquireSchemaReadLock();
    try {
      return description;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getStreamableName() {
    acquireSchemaReadLock();
    try {
      return shortName != null ? shortName : name;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<YTProperty> declaredProperties() {
    acquireSchemaReadLock();
    try {
      return Collections.unmodifiableCollection(properties.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Map<String, YTProperty> propertiesMap(YTDatabaseSession session) {
    ((YTDatabaseSessionInternal) session).checkSecurity(ORule.ResourceGeneric.SCHEMA,
        ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final Map<String, YTProperty> props = new HashMap<String, YTProperty>(20);
      propertiesMap(props);
      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void propertiesMap(Map<String, YTProperty> propertiesMap) {
    for (YTProperty p : properties.values()) {
      String propName = p.getName();
      if (!propertiesMap.containsKey(propName)) {
        propertiesMap.put(propName, p);
      }
    }
    for (YTClassImpl superClass : superClasses) {
      superClass.propertiesMap(propertiesMap);
    }
  }

  public Collection<YTProperty> properties(YTDatabaseSession session) {
    ((YTDatabaseSessionInternal) session).checkSecurity(ORule.ResourceGeneric.SCHEMA,
        ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final Collection<YTProperty> props = new ArrayList<YTProperty>();
      properties(props);
      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void properties(Collection<YTProperty> properties) {
    properties.addAll(this.properties.values());
    for (YTClassImpl superClass : superClasses) {
      superClass.properties(properties);
    }
  }

  public void getIndexedProperties(YTDatabaseSessionInternal session,
      Collection<YTProperty> indexedProperties) {
    for (YTProperty p : properties.values()) {
      if (areIndexed(session, p.getName())) {
        indexedProperties.add(p);
      }
    }
    for (YTClassImpl superClass : superClasses) {
      superClass.getIndexedProperties(session, indexedProperties);
    }
  }

  @Override
  public Collection<YTProperty> getIndexedProperties(YTDatabaseSession session) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA,
        ORole.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      Collection<YTProperty> indexedProps = new HashSet<YTProperty>();
      getIndexedProperties(sessionInternal, indexedProps);
      return indexedProps;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public YTProperty getProperty(String propertyName) {
    acquireSchemaReadLock();
    try {

      YTProperty p = properties.get(propertyName);
      if (p != null) {
        return p;
      }
      for (int i = 0; i < superClasses.size() && p == null; i++) {
        p = superClasses.get(i).getProperty(propertyName);
      }
      return p;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public YTProperty createProperty(YTDatabaseSession session, final String iPropertyName,
      final YTType iType) {
    return addProperty((YTDatabaseSessionInternal) session, iPropertyName, iType, null, null,
        false);
  }

  public YTProperty createProperty(
      YTDatabaseSession session, final String iPropertyName, final YTType iType,
      final YTClass iLinkedClass) {
    return addProperty((YTDatabaseSessionInternal) session, iPropertyName, iType, null,
        iLinkedClass,
        false);
  }

  public YTProperty createProperty(
      YTDatabaseSession session, final String iPropertyName,
      final YTType iType,
      final YTClass iLinkedClass,
      final boolean unsafe) {
    return addProperty((YTDatabaseSessionInternal) session, iPropertyName, iType, null,
        iLinkedClass,
        unsafe);
  }

  public YTProperty createProperty(
      YTDatabaseSession session, final String iPropertyName, final YTType iType,
      final YTType iLinkedType) {
    return addProperty((YTDatabaseSessionInternal) session, iPropertyName, iType, iLinkedType, null,
        false);
  }

  public YTProperty createProperty(
      YTDatabaseSession session, final String iPropertyName,
      final YTType iType,
      final YTType iLinkedType,
      final boolean unsafe) {
    return addProperty((YTDatabaseSessionInternal) session, iPropertyName, iType, iLinkedType, null,
        unsafe);
  }

  @Override
  public boolean existsProperty(String propertyName) {
    acquireSchemaReadLock();
    try {
      boolean result = properties.containsKey(propertyName);
      if (result) {
        return true;
      }
      for (YTClassImpl superClass : superClasses) {
        result = superClass.existsProperty(propertyName);
        if (result) {
          return true;
        }
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void fromStream(YTEntityImpl document) {
    subclasses = null;
    superClasses.clear();

    name = document.field("name");
    if (document.containsField("shortName")) {
      shortName = document.field("shortName");
    } else {
      shortName = null;
    }
    if (document.containsField("description")) {
      description = document.field("description");
    } else {
      description = null;
    }
    defaultClusterId = document.field("defaultClusterId");
    if (document.containsField("strictMode")) {
      strictMode = document.field("strictMode");
    } else {
      strictMode = false;
    }

    if (document.containsField("abstract")) {
      abstractClass = document.field("abstract");
    } else {
      abstractClass = false;
    }

    if (document.field("overSize") != null) {
      overSize = document.field("overSize");
    } else {
      overSize = 0f;
    }

    final Object cc = document.field("clusterIds");
    if (cc instanceof Collection<?>) {
      final Collection<Integer> coll = document.field("clusterIds");
      clusterIds = new int[coll.size()];
      int i = 0;
      for (final Integer item : coll) {
        clusterIds[i++] = item;
      }
    } else {
      clusterIds = (int[]) cc;
    }
    Arrays.sort(clusterIds);

    if (clusterIds.length == 1 && clusterIds[0] == -1) {
      setPolymorphicClusterIds(OCommonConst.EMPTY_INT_ARRAY);
    } else {
      setPolymorphicClusterIds(clusterIds);
    }

    // READ PROPERTIES
    YTPropertyImpl prop;

    final Map<String, YTProperty> newProperties = new HashMap<String, YTProperty>();
    final Collection<YTEntityImpl> storedProperties = document.field("properties");

    if (storedProperties != null) {
      for (YTIdentifiable id : storedProperties) {
        YTEntityImpl p = id.getRecord();
        String name = p.field("name");
        // To lower case ?
        if (properties.containsKey(name)) {
          prop = (YTPropertyImpl) properties.get(name);
          prop.fromStream(p);
        } else {
          prop = createPropertyInstance();
          prop.fromStream(p);
        }

        newProperties.put(prop.getName(), prop);
      }
    }

    properties.clear();
    properties.putAll(newProperties);
    customFields = document.field("customFields", YTType.EMBEDDEDMAP);
    clusterSelection =
        owner.getClusterSelectionFactory().getStrategy(document.field("clusterSelection"));
  }

  protected abstract YTPropertyImpl createPropertyInstance();

  public YTEntityImpl toStream() {
    YTEntityImpl document = new YTEntityImpl();
    document.field("name", name);
    document.field("shortName", shortName);
    document.field("description", description);
    document.field("defaultClusterId", defaultClusterId);
    document.field("clusterIds", clusterIds);
    document.field("clusterSelection", clusterSelection.getName());
    document.field("overSize", overSize);
    document.field("strictMode", strictMode);
    document.field("abstract", abstractClass);

    final Set<YTEntityImpl> props = new LinkedHashSet<YTEntityImpl>();
    for (final YTProperty p : properties.values()) {
      props.add(((YTPropertyImpl) p).toStream());
    }
    document.field("properties", props, YTType.EMBEDDEDSET);

    if (superClasses.isEmpty()) {
      // Single super class is deprecated!
      document.field("superClass", null, YTType.STRING);
      document.field("superClasses", null, YTType.EMBEDDEDLIST);
    } else {
      // Single super class is deprecated!
      document.field("superClass", superClasses.get(0).getName(), YTType.STRING);
      List<String> superClassesNames = new ArrayList<String>();
      for (YTClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      document.field("superClasses", superClassesNames, YTType.EMBEDDEDLIST);
    }

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        YTType.EMBEDDEDMAP);

    return document;
  }

  @Override
  public int getClusterForNewInstance(final YTEntityImpl doc) {
    acquireSchemaReadLock();
    try {
      return clusterSelection.getCluster(this, doc);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int getDefaultClusterId() {
    acquireSchemaReadLock();
    try {
      return defaultClusterId;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int[] getClusterIds() {
    acquireSchemaReadLock();
    try {
      return clusterIds;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int[] getPolymorphicClusterIds() {
    acquireSchemaReadLock();
    try {
      return Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length);
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void setPolymorphicClusterIds(final int[] iClusterIds) {
    IntRBTreeSet set = new IntRBTreeSet(iClusterIds);
    polymorphicClusterIds = set.toIntArray();
  }

  public void renameProperty(final String iOldName, final String iNewName) {
    final YTProperty p = properties.remove(iOldName);
    if (p != null) {
      properties.put(iNewName, p);
    }
  }

  public static YTClass addClusters(YTDatabaseSessionInternal session, final YTClass cls,
      final int iClusters) {
    final String clusterBase = cls.getName().toLowerCase(Locale.ENGLISH) + "_";
    for (int i = 0; i < iClusters; ++i) {
      cls.addCluster(session, clusterBase + i);
    }
    return cls;
  }

  protected void truncateClusterInternal(
      final String clusterName, final YTDatabaseSessionInternal database) {
    database.truncateCluster(clusterName);
  }

  public Collection<YTClass> getSubclasses() {
    acquireSchemaReadLock();
    try {
      if (subclasses == null || subclasses.size() == 0) {
        return Collections.emptyList();
      }

      return Collections.unmodifiableCollection(subclasses);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Collection<YTClass> getAllSubclasses() {
    acquireSchemaReadLock();
    try {
      final Set<YTClass> set = new HashSet<YTClass>();
      if (subclasses != null) {
        set.addAll(subclasses);

        for (YTClass c : subclasses) {
          set.addAll(c.getAllSubclasses());
        }
      }
      return set;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Deprecated
  public Collection<YTClass> getBaseClasses() {
    return getSubclasses();
  }

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
    for (YTClassImpl superClass : superClasses) {
      superClass.getAllSuperClasses(set);
    }
  }

  public abstract YTClass removeBaseClassInternal(YTDatabaseSessionInternal session,
      final YTClass baseClass);

  public float getOverSize() {
    acquireSchemaReadLock();
    try {
      if (overSize > 0)
      // CUSTOM OVERSIZE SET
      {
        return overSize;
      }

      // NO OVERSIZE by default
      float maxOverSize = 0;
      float thisOverSize;
      for (YTClassImpl superClass : superClasses) {
        thisOverSize = superClass.getOverSize();
        if (thisOverSize > maxOverSize) {
          maxOverSize = thisOverSize;
        }
      }
      return maxOverSize;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public float getClassOverSize() {
    acquireSchemaReadLock();
    try {
      return overSize;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean isAbstract() {
    acquireSchemaReadLock();
    try {
      return abstractClass;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean isStrictMode() {
    acquireSchemaReadLock();
    try {
      return strictMode;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public String toString() {
    acquireSchemaReadLock();
    try {
      return name;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public boolean equals(Object obj) {
    acquireSchemaReadLock();
    try {
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
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public int hashCode() {
    String name = this.name;
    if (name != null) {
      return name.hashCode();
    }
    return 0;
  }

  public int compareTo(final YTClass o) {
    acquireSchemaReadLock();
    try {
      return name.compareTo(o.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public long count(YTDatabaseSession session) {
    return count(session, true);
  }

  public long count(YTDatabaseSession session, final boolean isPolymorphic) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    acquireSchemaReadLock();
    try {
      return sessionInternal.countClass(getName(), isPolymorphic);
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Truncates all the clusters the class uses.
   */
  public void truncate(YTDatabaseSession session) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.truncateClass(name, false);
  }

  /**
   * Check if the current instance extends specified schema class.
   *
   * @param iClassName of class that should be checked
   * @return Returns true if the current instance extends the passed schema class (iClass)
   * @see #isSuperClassOf(YTClass)
   */
  public boolean isSubClassOf(final String iClassName) {
    acquireSchemaReadLock();
    try {
      if (iClassName == null) {
        return false;
      }

      if (iClassName.equalsIgnoreCase(getName()) || iClassName.equalsIgnoreCase(getShortName())) {
        return true;
      }
      for (YTClassImpl superClass : superClasses) {
        if (superClass.isSubClassOf(iClassName)) {
          return true;
        }
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Check if the current instance extends specified schema class.
   *
   * @param clazz to check
   * @return true if the current instance extends the passed schema class (iClass)
   * @see #isSuperClassOf(YTClass)
   */
  public boolean isSubClassOf(final YTClass clazz) {
    acquireSchemaReadLock();
    try {
      if (clazz == null) {
        return false;
      }
      if (equals(clazz)) {
        return true;
      }
      for (YTClassImpl superClass : superClasses) {
        if (superClass.isSubClassOf(clazz)) {
          return true;
        }
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Returns true if the passed schema class (iClass) extends the current instance.
   *
   * @param clazz to check
   * @return Returns true if the passed schema class extends the current instance
   * @see #isSubClassOf(YTClass)
   */
  public boolean isSuperClassOf(final YTClass clazz) {
    return clazz != null && clazz.isSubClassOf(this);
  }

  public Object get(final ATTRIBUTES iAttribute) {
    if (iAttribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    switch (iAttribute) {
      case NAME:
        return getName();
      case SHORTNAME:
        return getShortName();
      case SUPERCLASS:
        return getSuperClass();
      case SUPERCLASSES:
        return getSuperClasses();
      case OVERSIZE:
        return getOverSize();
      case STRICTMODE:
        return isStrictMode();
      case ABSTRACT:
        return isAbstract();
      case CLUSTERSELECTION:
        return getClusterSelection();
      case CUSTOM:
        return getCustomInternal();
      case DESCRIPTION:
        return getDescription();
    }

    throw new IllegalArgumentException("Cannot find attribute '" + iAttribute + "'");
  }

  public YTClass set(YTDatabaseSession session, final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    final String stringValue = iValue != null ? iValue.toString() : null;
    final boolean isNull = stringValue == null || stringValue.equalsIgnoreCase("NULL");

    var sessionInternal = (YTDatabaseSessionInternal) session;
    switch (attribute) {
      case NAME:
        setName(session, decodeClassName(stringValue));
        break;
      case SHORTNAME:
        setShortName(session, decodeClassName(stringValue));
        break;
      case SUPERCLASS:
        if (stringValue == null) {
          throw new IllegalArgumentException("Superclass is null");
        }

        if (stringValue.startsWith("+")) {
          addSuperClass(session,
              sessionInternal
                  .getMetadata()
                  .getSchema()
                  .getClass(decodeClassName(stringValue.substring(1))));
        } else if (stringValue.startsWith("-")) {
          removeSuperClass(session,
              sessionInternal
                  .getMetadata()
                  .getSchema()
                  .getClass(decodeClassName(stringValue.substring(1))));
        } else {
          setSuperClass(sessionInternal,
              sessionInternal.getMetadata().getSchema().getClass(decodeClassName(stringValue)));
        }
        break;
      case SUPERCLASSES:
        setSuperClassesByNames(sessionInternal
            , stringValue != null ? Arrays.asList(stringValue.split(",\\s*")) : null);
        break;
      case OVERSIZE:
        setOverSize(session, Float.parseFloat(stringValue));
        break;
      case STRICTMODE:
        setStrictMode(session, Boolean.parseBoolean(stringValue));
        break;
      case ABSTRACT:
        setAbstract(session, Boolean.parseBoolean(stringValue));
        break;
      case ADDCLUSTER: {
        addCluster(session, stringValue);
        break;
      }
      case REMOVECLUSTER:
        int clId = owner.getClusterId(sessionInternal, stringValue);
        if (clId == NOT_EXISTENT_CLUSTER_ID) {
          throw new IllegalArgumentException("Cluster id '" + stringValue + "' cannot be removed");
        }
        removeClusterId(session, clId);
        break;
      case CLUSTERSELECTION:
        setClusterSelection(session, stringValue);
        break;
      case CUSTOM:
        int indx = stringValue != null ? stringValue.indexOf('=') : -1;
        if (indx < 0) {
          if (isNull || "clear".equalsIgnoreCase(stringValue)) {
            clearCustom(session);
          } else {
            throw new IllegalArgumentException(
                "Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
          }
        } else {
          String customName = stringValue.substring(0, indx).trim();
          String customValue = stringValue.substring(indx + 1).trim();
          if (isQuoted(customValue)) {
            customValue = removeQuotes(customValue);
          }
          if (customValue.isEmpty()) {
            removeCustom(sessionInternal, customName);
          } else {
            setCustom(session, customName, customValue);
          }
        }
        break;
      case DESCRIPTION:
        setDescription(session, stringValue);
        break;
      case ENCRYPTION:
        setEncryption(sessionInternal, stringValue);
        break;
    }
    return this;
  }

  private String removeQuotes(String s) {
    s = s.trim();
    return s.substring(1, s.length() - 1);
  }

  private boolean isQuoted(String s) {
    s = s.trim();
    if (s.startsWith("\"") && s.endsWith("\"")) {
      return true;
    }
    if (s.startsWith("'") && s.endsWith("'")) {
      return true;
    }
    return s.startsWith("`") && s.endsWith("`");
  }

  public abstract YTClassImpl setEncryption(YTDatabaseSessionInternal session, final String iValue);

  public OIndex createIndex(YTDatabaseSession session, final String iName, final INDEX_TYPE iType,
      final String... fields) {
    return createIndex(session, iName, iType.name(), fields);
  }

  public OIndex createIndex(YTDatabaseSession session, final String iName, final String iType,
      final String... fields) {
    return createIndex(session, iName, iType, null, null, fields);
  }

  public OIndex createIndex(
      YTDatabaseSession session, final String iName,
      final INDEX_TYPE iType,
      final OProgressListener iProgressListener,
      final String... fields) {
    return createIndex(session, iName, iType.name(), iProgressListener, null, fields);
  }

  public OIndex createIndex(
      YTDatabaseSession session, String iName,
      String iType,
      OProgressListener iProgressListener,
      YTEntityImpl metadata,
      String... fields) {
    return createIndex(session, iName, iType, iProgressListener, metadata, null, fields);
  }

  public OIndex createIndex(
      YTDatabaseSession session, final String name,
      String type,
      final OProgressListener progressListener,
      YTEntityImpl metadata,
      String algorithm,
      final String... fields) {
    if (type == null) {
      throw new IllegalArgumentException("Index type is null");
    }

    type = type.toUpperCase(Locale.ENGLISH);

    if (fields.length == 0) {
      throw new YTIndexException("List of fields to index cannot be empty.");
    }

    var sessionInternal = (YTDatabaseSessionInternal) session;
    final String localName = this.name;
    final int[] localPolymorphicClusterIds = polymorphicClusterIds;

    for (final String fieldToIndex : fields) {
      final String fieldName =
          decodeClassName(OIndexDefinitionFactory.extractFieldName(fieldToIndex));

      if (!fieldName.equals("@rid") && !existsProperty(fieldName)) {
        throw new YTIndexException(
            "Index with name '"
                + name
                + "' cannot be created on class '"
                + localName
                + "' because the field '"
                + fieldName
                + "' is absent in class definition");
      }
    }

    final OIndexDefinition indexDefinition =
        OIndexDefinitionFactory.createIndexDefinition(
            this, Arrays.asList(fields), extractFieldTypes(fields), null, type, algorithm);

    return sessionInternal
        .getMetadata()
        .getIndexManagerInternal()
        .createIndex(
            sessionInternal,
            name,
            type,
            indexDefinition,
            localPolymorphicClusterIds,
            progressListener,
            metadata,
            algorithm);
  }

  public boolean areIndexed(YTDatabaseSession session, final String... fields) {
    return areIndexed(session, Arrays.asList(fields));
  }

  public boolean areIndexed(YTDatabaseSession session, final Collection<String> fields) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    final OIndexManagerAbstract indexManager =
        sessionInternal.getMetadata().getIndexManagerInternal();

    acquireSchemaReadLock();
    try {
      final boolean currentClassResult = indexManager.areIndexed(name, fields);

      if (currentClassResult) {
        return true;
      }
      for (YTClassImpl superClass : superClasses) {
        if (superClass.areIndexed(sessionInternal, fields)) {
          return true;
        }
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getInvolvedIndexes(YTDatabaseSession session, final String... fields) {
    return getInvolvedIndexes(session, Arrays.asList(fields));
  }

  public Set<OIndex> getInvolvedIndexes(YTDatabaseSession session,
      final Collection<String> fields) {
    acquireSchemaReadLock();
    try {
      final Set<OIndex> result = new HashSet<OIndex>(getClassInvolvedIndexes(session, fields));

      for (YTClassImpl superClass : superClasses) {
        result.addAll(superClass.getInvolvedIndexes(session, fields));
      }

      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getClassInvolvedIndexes(YTDatabaseSession session,
      final Collection<String> fields) {
    final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
    final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    acquireSchemaReadLock();
    try {
      return indexManager.getClassInvolvedIndexes(database, name, fields);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getClassInvolvedIndexes(YTDatabaseSession session, final String... fields) {
    return getClassInvolvedIndexes(session, Arrays.asList(fields));
  }

  public OIndex getClassIndex(YTDatabaseSession session, final String name) {
    acquireSchemaReadLock();
    try {
      final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
      return database
          .getMetadata()
          .getIndexManagerInternal()
          .getClassIndex(database, this.name, name);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getClassIndexes(YTDatabaseSession session) {
    acquireSchemaReadLock();
    try {
      final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
      final OIndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
      if (idxManager == null) {
        return new HashSet<>();
      }

      return idxManager.getClassIndexes(database, name);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public void getClassIndexes(YTDatabaseSession session, final Collection<OIndex> indexes) {
    acquireSchemaReadLock();
    try {
      final YTDatabaseSessionInternal database = (YTDatabaseSessionInternal) session;
      final OIndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
      if (idxManager == null) {
        return;
      }

      idxManager.getClassIndexes(database, name, indexes);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public OIndex getAutoShardingIndex(YTDatabaseSession session) {
    final YTDatabaseSessionInternal db = (YTDatabaseSessionInternal) session;
    return db != null
        ? db.getMetadata().getIndexManagerInternal().getClassAutoShardingIndex(db, name)
        : null;
  }

  @Override
  public boolean isEdgeType() {
    return isSubClassOf(EDGE_CLASS_NAME);
  }

  @Override
  public boolean isVertexType() {
    return isSubClassOf(VERTEX_CLASS_NAME);
  }

  public void onPostIndexManagement(YTDatabaseSessionInternal session) {
    final OIndex autoShardingIndex = getAutoShardingIndex(session);
    if (autoShardingIndex != null) {
      if (!session.isRemote()) {
        // OVERRIDE CLUSTER SELECTION
        acquireSchemaWriteLock(session);
        try {
          this.clusterSelection =
              new OAutoShardingClusterSelectionStrategy(this, autoShardingIndex);
        } finally {
          releaseSchemaWriteLock(session);
        }
      }
    } else if (clusterSelection instanceof OAutoShardingClusterSelectionStrategy) {
      // REMOVE AUTO SHARDING CLUSTER SELECTION
      acquireSchemaWriteLock(session);
      try {
        this.clusterSelection = owner.getClusterSelectionFactory().newInstanceOfDefaultClass();
      } finally {
        releaseSchemaWriteLock(session);
      }
    }
  }

  @Override
  public void getIndexes(YTDatabaseSession session, final Collection<OIndex> indexes) {
    acquireSchemaReadLock();
    try {
      getClassIndexes(session, indexes);
      for (YTClass superClass : superClasses) {
        superClass.getIndexes(session, indexes);
      }
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<OIndex> getIndexes(YTDatabaseSession session) {
    final Set<OIndex> indexes = new HashSet<OIndex>();
    getIndexes(session, indexes);
    return indexes;
  }

  public void acquireSchemaReadLock() {
    owner.acquireSchemaReadLock();
  }

  public void releaseSchemaReadLock() {
    owner.releaseSchemaReadLock();
  }

  public void acquireSchemaWriteLock(YTDatabaseSessionInternal session) {
    owner.acquireSchemaWriteLock(session);
  }

  public void releaseSchemaWriteLock(YTDatabaseSessionInternal session) {
    releaseSchemaWriteLock(session, true);
  }

  public void releaseSchemaWriteLock(YTDatabaseSessionInternal session, final boolean iSave) {
    calculateHashCode();
    owner.releaseSchemaWriteLock(session, iSave);
  }

  public void checkEmbedded() {
    owner.checkEmbedded();
  }

  public void setClusterSelectionInternal(final OClusterSelectionStrategy iClusterSelection) {
    // AVOID TO CHECK THIS IN LOCK TO AVOID RE-GENERATION OF IMMUTABLE SCHEMAS
    if (this.clusterSelection.getClass().equals(iClusterSelection.getClass()))
    // NO CHANGES
    {
      return;
    }

    // DON'T GET THE SCHEMA LOCK BECAUSE THIS CHANGE IS USED ONLY TO WRAP THE SELECTION STRATEGY
    checkEmbedded();
    this.clusterSelection = iClusterSelection;
  }

  public void fireDatabaseMigration(
      final YTDatabaseSession database, final String propertyName, final YTType type) {
    final boolean strictSQL =
        ((YTDatabaseSessionInternal) database).getStorageInfo().getConfiguration().isStrictSql();

    try (YTResultSet result =
        database.query(
            "select from "
                + getEscapedName(name, strictSQL)
                + " where "
                + getEscapedName(propertyName, strictSQL)
                + ".type() <> \""
                + type.name()
                + "\"")) {
      while (result.hasNext()) {
        database.executeInTx(
            () -> {
              YTEntityImpl record =
                  database.bindToSession((YTEntityImpl) result.next().getEntity().get());
              record.field(propertyName, record.field(propertyName), type);
              database.save(record);
            });
      }
    }
  }

  public void firePropertyNameMigration(
      final YTDatabaseSession database,
      final String propertyName,
      final String newPropertyName,
      final YTType type) {
    final boolean strictSQL =
        ((YTDatabaseSessionInternal) database).getStorageInfo().getConfiguration().isStrictSql();

    try (YTResultSet result =
        database.query(
            "select from "
                + getEscapedName(name, strictSQL)
                + " where "
                + getEscapedName(propertyName, strictSQL)
                + " is not null ")) {
      while (result.hasNext()) {
        database.executeInTx(
            () -> {
              YTEntityImpl record =
                  database.bindToSession((YTEntityImpl) result.next().getEntity().get());
              record.setFieldType(propertyName, type);
              record.field(newPropertyName, record.field(propertyName), type);
              database.save(record);
            });
      }
    }
  }

  public void checkPersistentPropertyType(
      final YTDatabaseSessionInternal database,
      final String propertyName,
      final YTType type,
      YTClass linkedClass) {
    if (YTType.ANY.equals(type)) {
      return;
    }
    final boolean strictSQL = database.getStorageInfo().getConfiguration().isStrictSql();

    final StringBuilder builder = new StringBuilder(256);
    builder.append("select from ");
    builder.append(getEscapedName(name, strictSQL));
    builder.append(" where ");
    builder.append(getEscapedName(propertyName, strictSQL));
    builder.append(".type() not in [");

    final Iterator<YTType> cur = type.getCastable().iterator();
    while (cur.hasNext()) {
      builder.append('"').append(cur.next().name()).append('"');
      if (cur.hasNext()) {
        builder.append(",");
      }
    }
    builder
        .append("] and ")
        .append(getEscapedName(propertyName, strictSQL))
        .append(" is not null ");
    if (type.isMultiValue()) {
      builder
          .append(" and ")
          .append(getEscapedName(propertyName, strictSQL))
          .append(".size() <> 0 limit 1");
    }

    try (final YTResultSet res = database.command(builder.toString())) {
      if (res.hasNext()) {
        throw new YTSchemaException(
            "The database contains some schema-less data in the property '"
                + name
                + "."
                + propertyName
                + "' that is not compatible with the type "
                + type
                + ". Fix those records and change the schema again");
      }
    }

    if (linkedClass != null) {
      checkAllLikedObjects(database, propertyName, type, linkedClass);
    }
  }

  protected void checkAllLikedObjects(
      YTDatabaseSessionInternal database, String propertyName, YTType type, YTClass linkedClass) {
    final StringBuilder builder = new StringBuilder(256);
    builder.append("select from ");
    builder.append(getEscapedName(name, true));
    builder.append(" where ");
    builder.append(getEscapedName(propertyName, true)).append(" is not null ");
    if (type.isMultiValue()) {
      builder.append(" and ").append(getEscapedName(propertyName, true)).append(".size() > 0");
    }

    try (final YTResultSet res = database.command(builder.toString())) {
      while (res.hasNext()) {
        YTResult item = res.next();
        switch (type) {
          case EMBEDDEDLIST:
          case LINKLIST:
          case EMBEDDEDSET:
          case LINKSET:
            try {
              Collection emb = item.toEntity().getProperty(propertyName);
              emb.stream()
                  .filter(x -> !matchesType(x, linkedClass))
                  .findFirst()
                  .ifPresent(
                      x -> {
                        throw new YTSchemaException(
                            "The database contains some schema-less data in the property '"
                                + name
                                + "."
                                + propertyName
                                + "' that is not compatible with the type "
                                + type
                                + " "
                                + linkedClass.getName()
                                + ". Fix those records and change the schema again. "
                                + x);
                      });
            } catch (YTSchemaException e1) {
              throw e1;
            } catch (Exception e) {
            }
            break;
          case EMBEDDED:
          case LINK:
            Object elem = item.getProperty(propertyName);
            if (!matchesType(elem, linkedClass)) {
              throw new YTSchemaException(
                  "The database contains some schema-less data in the property '"
                      + name
                      + "."
                      + propertyName
                      + "' that is not compatible with the type "
                      + type
                      + " "
                      + linkedClass.getName()
                      + ". Fix those records and change the schema again!");
            }
            break;
        }
      }
    }
  }

  protected static boolean matchesType(Object x, YTClass linkedClass) {
    if (x instanceof YTResult) {
      x = ((YTResult) x).toEntity();
    }
    if (x instanceof YTRID) {
      try {
        x = ((YTRID) x).getRecord();
      } catch (YTRecordNotFoundException e) {
        return true;
      }
    }
    if (x == null) {
      return true;
    }
    if (!(x instanceof YTEntity)) {
      return false;
    }
    return !(x instanceof YTEntityImpl)
        || linkedClass.getName().equalsIgnoreCase(((YTEntityImpl) x).getClassName());
  }

  protected static String getEscapedName(final String iName, final boolean iStrictSQL) {
    if (iStrictSQL)
    // ESCAPE NAME
    {
      return "`" + iName + "`";
    }
    return iName;
  }

  public OSchemaShared getOwner() {
    return owner;
  }

  private void calculateHashCode() {
    int result = super.hashCode();
    result = 31 * result + (name != null ? name.hashCode() : 0);
    hashCode = result;
  }

  protected void renameCluster(YTDatabaseSessionInternal session, String oldName, String newName) {
    oldName = oldName.toLowerCase(Locale.ENGLISH);
    newName = newName.toLowerCase(Locale.ENGLISH);

    if (session.getClusterIdByName(newName) != -1) {
      return;
    }

    final int clusterId = session.getClusterIdByName(oldName);
    if (clusterId == -1) {
      return;
    }

    if (!hasClusterId(clusterId)) {
      return;
    }

    session.command("alter cluster `" + oldName + "` NAME \"" + newName + "\"").close();
  }

  protected void onlyAddPolymorphicClusterId(int clusterId) {
    if (Arrays.binarySearch(polymorphicClusterIds, clusterId) >= 0) {
      return;
    }

    polymorphicClusterIds = OArrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length + 1);
    polymorphicClusterIds[polymorphicClusterIds.length - 1] = clusterId;
    Arrays.sort(polymorphicClusterIds);

    for (YTClassImpl superClass : superClasses) {
      superClass.onlyAddPolymorphicClusterId(clusterId);
    }
  }

  protected abstract YTProperty addProperty(
      YTDatabaseSessionInternal session, final String propertyName,
      final YTType type,
      final YTType linkedType,
      final YTClass linkedClass,
      final boolean unsafe);

  protected void validatePropertyName(final String propertyName) {
  }

  protected abstract void addClusterIdToIndexes(YTDatabaseSessionInternal session, int iId);

  /**
   * Adds a base class to the current one. It adds also the base class cluster ids to the
   * polymorphic cluster ids array.
   *
   * @param session
   * @param iBaseClass The base class to add.
   */
  public YTClass addBaseClass(YTDatabaseSessionInternal session, final YTClassImpl iBaseClass) {
    checkRecursion(iBaseClass);

    if (subclasses == null) {
      subclasses = new ArrayList<YTClass>();
    }

    if (subclasses.contains(iBaseClass)) {
      return this;
    }

    subclasses.add(iBaseClass);
    addPolymorphicClusterIdsWithInheritance(session, iBaseClass);
    return this;
  }

  protected void checkParametersConflict(YTDatabaseSessionInternal session,
      final YTClass baseClass) {
    final Collection<YTProperty> baseClassProperties = baseClass.properties(session);
    for (YTProperty property : baseClassProperties) {
      YTProperty thisProperty = getProperty(property.getName());
      if (thisProperty != null && !thisProperty.getType().equals(property.getType())) {
        throw new YTSchemaException(
            "Cannot add base class '"
                + baseClass.getName()
                + "', because of property conflict: '"
                + thisProperty
                + "' vs '"
                + property
                + "'");
      }
    }
  }

  public static void checkParametersConflict(List<YTClass> classes) {
    final Map<String, YTProperty> comulative = new HashMap<String, YTProperty>();
    final Map<String, YTProperty> properties = new HashMap<String, YTProperty>();

    for (YTClass superClass : classes) {
      if (superClass == null) {
        continue;
      }
      YTClassImpl impl;

      if (superClass instanceof YTClassAbstractDelegate) {
        impl = (YTClassImpl) ((YTClassAbstractDelegate) superClass).delegate;
      } else {
        impl = (YTClassImpl) superClass;
      }
      impl.propertiesMap(properties);
      for (Map.Entry<String, YTProperty> entry : properties.entrySet()) {
        if (comulative.containsKey(entry.getKey())) {
          final String property = entry.getKey();
          final YTProperty existingProperty = comulative.get(property);
          if (!existingProperty.getType().equals(entry.getValue().getType())) {
            throw new YTSchemaException(
                "Properties conflict detected: '"
                    + existingProperty
                    + "] vs ["
                    + entry.getValue()
                    + "]");
          }
        }
      }

      comulative.putAll(properties);
      properties.clear();
    }
  }

  private void checkRecursion(final YTClass baseClass) {
    if (isSubClassOf(baseClass)) {
      throw new YTSchemaException(
          "Cannot add base class '" + baseClass.getName() + "', because of recursion");
    }
  }

  protected void removePolymorphicClusterIds(YTDatabaseSessionInternal session,
      final YTClassImpl iBaseClass) {
    for (final int clusterId : iBaseClass.polymorphicClusterIds) {
      removePolymorphicClusterId(session, clusterId);
    }
  }

  protected void onlyRemovePolymorphicClusterId(final int clusterId) {
    final int index = Arrays.binarySearch(polymorphicClusterIds, clusterId);
    if (index < 0) {
      return;
    }

    if (index < polymorphicClusterIds.length - 1) {
      System.arraycopy(
          polymorphicClusterIds,
          index + 1,
          polymorphicClusterIds,
          index,
          polymorphicClusterIds.length - (index + 1));
    }

    polymorphicClusterIds = Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length - 1);

    for (YTClassImpl superClass : superClasses) {
      superClass.onlyRemovePolymorphicClusterId(clusterId);
    }
  }

  protected void removePolymorphicClusterId(YTDatabaseSessionInternal session,
      final int clusterId) {
    final int index = Arrays.binarySearch(polymorphicClusterIds, clusterId);
    if (index < 0) {
      return;
    }

    if (index < polymorphicClusterIds.length - 1) {
      System.arraycopy(
          polymorphicClusterIds,
          index + 1,
          polymorphicClusterIds,
          index,
          polymorphicClusterIds.length - (index + 1));
    }

    polymorphicClusterIds = Arrays.copyOf(polymorphicClusterIds, polymorphicClusterIds.length - 1);

    removeClusterFromIndexes(session, clusterId);
    for (YTClassImpl superClass : superClasses) {
      superClass.removePolymorphicClusterId(session, clusterId);
    }
  }

  private void removeClusterFromIndexes(YTDatabaseSessionInternal session, final int iId) {
    if (session.getStorage() instanceof OAbstractPaginatedStorage) {
      final String clusterName = session.getClusterNameById(iId);
      final List<String> indexesToRemove = new ArrayList<String>();

      for (final OIndex index : getIndexes(session)) {
        indexesToRemove.add(index.getName());
      }

      final OIndexManagerAbstract indexManager =
          session.getMetadata().getIndexManagerInternal();
      for (final String indexName : indexesToRemove) {
        indexManager.removeClusterFromIndex(session, clusterName, indexName);
      }
    }
  }

  /**
   * Add different cluster id to the "polymorphic cluster ids" array.
   */
  protected void addPolymorphicClusterIds(YTDatabaseSessionInternal session,
      final YTClassImpl iBaseClass) {
    IntRBTreeSet clusters = new IntRBTreeSet(polymorphicClusterIds);

    for (int clusterId : iBaseClass.polymorphicClusterIds) {
      if (clusters.add(clusterId)) {
        try {
          addClusterIdToIndexes(session, clusterId);
        } catch (RuntimeException e) {
          OLogManager.instance()
              .warn(
                  this,
                  "Error adding clusterId '%d' to index of class '%s'",
                  e,
                  clusterId,
                  getName());
          clusters.remove(clusterId);
        }
      }
    }

    polymorphicClusterIds = clusters.toIntArray();
  }

  private void addPolymorphicClusterIdsWithInheritance(YTDatabaseSessionInternal session,
      final YTClassImpl iBaseClass) {
    addPolymorphicClusterIds(session, iBaseClass);
    for (YTClassImpl superClass : superClasses) {
      superClass.addPolymorphicClusterIdsWithInheritance(session, iBaseClass);
    }
  }

  public List<YTType> extractFieldTypes(final String[] fieldNames) {
    final List<YTType> types = new ArrayList<YTType>(fieldNames.length);

    for (String fieldName : fieldNames) {
      if (!fieldName.equals("@rid")) {
        types.add(
            getProperty(decodeClassName(OIndexDefinitionFactory.extractFieldName(fieldName)))
                .getType());
      } else {
        types.add(YTType.LINK);
      }
    }
    return types;
  }

  protected YTClass setClusterIds(final int[] iClusterIds) {
    clusterIds = iClusterIds;
    Arrays.sort(clusterIds);

    return this;
  }

  public static String decodeClassName(String s) {
    if (s == null) {
      return null;
    }
    s = s.trim();
    if (s.startsWith("`") && s.endsWith("`")) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  public YTEntityImpl toNetworkStream() {
    YTEntityImpl document = new YTEntityImpl();
    document.setTrackingChanges(false);
    document.field("name", name);
    document.field("shortName", shortName);
    document.field("description", description);
    document.field("defaultClusterId", defaultClusterId);
    document.field("clusterIds", clusterIds);
    document.field("clusterSelection", clusterSelection.getName());
    document.field("overSize", overSize);
    document.field("strictMode", strictMode);
    document.field("abstract", abstractClass);

    final Set<YTEntityImpl> props = new LinkedHashSet<YTEntityImpl>();
    for (final YTProperty p : properties.values()) {
      props.add(((YTPropertyImpl) p).toNetworkStream());
    }
    document.field("properties", props, YTType.EMBEDDEDSET);

    if (superClasses.isEmpty()) {
      // Single super class is deprecated!
      document.field("superClass", null, YTType.STRING);
      document.field("superClasses", null, YTType.EMBEDDEDLIST);
    } else {
      // Single super class is deprecated!
      document.field("superClass", superClasses.get(0).getName(), YTType.STRING);
      List<String> superClassesNames = new ArrayList<String>();
      for (YTClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      document.field("superClasses", superClassesNames, YTType.EMBEDDEDLIST);
    }

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        YTType.EMBEDDEDMAP);

    return document;
  }
}
