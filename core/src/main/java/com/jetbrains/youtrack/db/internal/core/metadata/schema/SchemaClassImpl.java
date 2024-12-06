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
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.ArrayUtils;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaException;
import com.jetbrains.youtrack.db.internal.core.exception.SecurityAccessException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinitionFactory;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection.ClusterSelectionStrategy;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sharding.auto.AutoShardingClusterSelectionStrategy;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
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
public abstract class SchemaClassImpl implements SchemaClass {

  protected static final int NOT_EXISTENT_CLUSTER_ID = -1;
  protected final SchemaShared owner;
  protected final Map<String, Property> properties = new HashMap<String, Property>();
  protected int defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
  protected String name;
  protected String description;
  protected int[] clusterIds;
  protected List<SchemaClassImpl> superClasses = new ArrayList<SchemaClassImpl>();
  protected int[] polymorphicClusterIds;
  protected List<SchemaClass> subclasses;
  protected float overSize = 0f;
  protected String shortName;
  protected boolean strictMode = false; // @SINCE v1.0rc8
  protected boolean abstractClass = false; // @SINCE v1.2.0
  protected Map<String, String> customFields;
  protected volatile ClusterSelectionStrategy clusterSelection; // @SINCE 1.7
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

  protected SchemaClassImpl(final SchemaShared iOwner, final String iName,
      final int[] iClusterIds) {
    this(iOwner, iName);
    setClusterIds(iClusterIds);
    defaultClusterId = iClusterIds[0];
    if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID) {
      abstractClass = true;
    }

    if (abstractClass) {
      setPolymorphicClusterIds(CommonConst.EMPTY_INT_ARRAY);
    } else {
      setPolymorphicClusterIds(iClusterIds);
    }

    clusterSelection = owner.getClusterSelectionFactory().newInstanceOfDefaultClass();
  }

  /**
   * Constructor used in unmarshalling.
   */
  protected SchemaClassImpl(final SchemaShared iOwner, final String iName) {
    name = iName;
    owner = iOwner;
  }

  public static int[] readableClusters(
      final DatabaseSessionInternal db, final int[] iClusterIds, String className) {
    IntArrayList listOfReadableIds = new IntArrayList();

    boolean all = true;
    for (int clusterId : iClusterIds) {
      try {
        // This will exclude (filter out) any specific classes without explicit read permission.
        if (className != null) {
          db.checkSecurity(Rule.ResourceGeneric.CLASS, Role.PERMISSION_READ, className);
        }

        final String clusterName = db.getClusterNameById(clusterId);
        db.checkSecurity(Rule.ResourceGeneric.CLUSTER, Role.PERMISSION_READ, clusterName);
        listOfReadableIds.add(clusterId);
      } catch (SecurityAccessException ignore) {
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
  public ClusterSelectionStrategy getClusterSelection() {
    acquireSchemaReadLock();
    try {
      return clusterSelection;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public SchemaClass setClusterSelection(DatabaseSession session,
      final ClusterSelectionStrategy clusterSelection) {
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

  public void removeCustom(DatabaseSession session, final String name) {
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
  public SchemaClass getSuperClass() {
    acquireSchemaReadLock();
    try {
      return superClasses.isEmpty() ? null : superClasses.get(0);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  @Deprecated
  public SchemaClass setSuperClass(DatabaseSession session, SchemaClass iSuperClass) {
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
  public List<SchemaClass> getSuperClasses() {
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
      for (SchemaClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      return superClassesNames;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void setSuperClassesByNames(DatabaseSessionInternal session, List<String> classNames) {
    if (classNames == null) {
      classNames = Collections.EMPTY_LIST;
    }

    final List<SchemaClass> classes = new ArrayList<SchemaClass>(classNames.size());
    final Schema schema = session.getMetadata().getSchema();
    for (String className : classNames) {
      classes.add(schema.getClass(decodeClassName(className)));
    }
    setSuperClasses(session, classes);
  }

  protected abstract void setSuperClassesInternal(DatabaseSessionInternal session,
      final List<? extends SchemaClass> classes);

  public long getSize(DatabaseSessionInternal session) {
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

  public Collection<Property> declaredProperties() {
    acquireSchemaReadLock();
    try {
      return Collections.unmodifiableCollection(properties.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Map<String, Property> propertiesMap(DatabaseSession session) {
    ((DatabaseSessionInternal) session).checkSecurity(Rule.ResourceGeneric.SCHEMA,
        Role.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final Map<String, Property> props = new HashMap<String, Property>(20);
      propertiesMap(props);
      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void propertiesMap(Map<String, Property> propertiesMap) {
    for (Property p : properties.values()) {
      String propName = p.getName();
      if (!propertiesMap.containsKey(propName)) {
        propertiesMap.put(propName, p);
      }
    }
    for (SchemaClassImpl superClass : superClasses) {
      superClass.propertiesMap(propertiesMap);
    }
  }

  public Collection<Property> properties(DatabaseSession session) {
    ((DatabaseSessionInternal) session).checkSecurity(Rule.ResourceGeneric.SCHEMA,
        Role.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final Collection<Property> props = new ArrayList<Property>();
      properties(props);
      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void properties(Collection<Property> properties) {
    properties.addAll(this.properties.values());
    for (SchemaClassImpl superClass : superClasses) {
      superClass.properties(properties);
    }
  }

  public void getIndexedProperties(DatabaseSessionInternal session,
      Collection<Property> indexedProperties) {
    for (Property p : properties.values()) {
      if (areIndexed(session, p.getName())) {
        indexedProperties.add(p);
      }
    }
    for (SchemaClassImpl superClass : superClasses) {
      superClass.getIndexedProperties(session, indexedProperties);
    }
  }

  @Override
  public Collection<Property> getIndexedProperties(DatabaseSession session) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA,
        Role.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      Collection<Property> indexedProps = new HashSet<Property>();
      getIndexedProperties(sessionInternal, indexedProps);
      return indexedProps;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Property getProperty(String propertyName) {
    acquireSchemaReadLock();
    try {

      Property p = properties.get(propertyName);
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

  public Property createProperty(DatabaseSession session, final String iPropertyName,
      final PropertyType iType) {
    return addProperty((DatabaseSessionInternal) session, iPropertyName, iType, null, null,
        false);
  }

  public Property createProperty(
      DatabaseSession session, final String iPropertyName, final PropertyType iType,
      final SchemaClass iLinkedClass) {
    return addProperty((DatabaseSessionInternal) session, iPropertyName, iType, null,
        iLinkedClass,
        false);
  }

  public Property createProperty(
      DatabaseSession session, final String iPropertyName,
      final PropertyType iType,
      final SchemaClass iLinkedClass,
      final boolean unsafe) {
    return addProperty((DatabaseSessionInternal) session, iPropertyName, iType, null,
        iLinkedClass,
        unsafe);
  }

  public Property createProperty(
      DatabaseSession session, final String iPropertyName, final PropertyType iType,
      final PropertyType iLinkedType) {
    return addProperty((DatabaseSessionInternal) session, iPropertyName, iType, iLinkedType, null,
        false);
  }

  public Property createProperty(
      DatabaseSession session, final String iPropertyName,
      final PropertyType iType,
      final PropertyType iLinkedType,
      final boolean unsafe) {
    return addProperty((DatabaseSessionInternal) session, iPropertyName, iType, iLinkedType, null,
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
      for (SchemaClassImpl superClass : superClasses) {
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

  public void fromStream(EntityImpl document) {
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
      setPolymorphicClusterIds(CommonConst.EMPTY_INT_ARRAY);
    } else {
      setPolymorphicClusterIds(clusterIds);
    }

    // READ PROPERTIES
    PropertyImpl prop;

    final Map<String, Property> newProperties = new HashMap<String, Property>();
    final Collection<EntityImpl> storedProperties = document.field("properties");

    if (storedProperties != null) {
      for (Identifiable id : storedProperties) {
        EntityImpl p = id.getRecord();
        String name = p.field("name");
        // To lower case ?
        if (properties.containsKey(name)) {
          prop = (PropertyImpl) properties.get(name);
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
    customFields = document.field("customFields", PropertyType.EMBEDDEDMAP);
    clusterSelection =
        owner.getClusterSelectionFactory().getStrategy(document.field("clusterSelection"));
  }

  protected abstract PropertyImpl createPropertyInstance();

  public EntityImpl toStream() {
    EntityImpl document = new EntityImpl();
    document.field("name", name);
    document.field("shortName", shortName);
    document.field("description", description);
    document.field("defaultClusterId", defaultClusterId);
    document.field("clusterIds", clusterIds);
    document.field("clusterSelection", clusterSelection.getName());
    document.field("overSize", overSize);
    document.field("strictMode", strictMode);
    document.field("abstract", abstractClass);

    final Set<EntityImpl> props = new LinkedHashSet<EntityImpl>();
    for (final Property p : properties.values()) {
      props.add(((PropertyImpl) p).toStream());
    }
    document.field("properties", props, PropertyType.EMBEDDEDSET);

    if (superClasses.isEmpty()) {
      // Single super class is deprecated!
      document.field("superClass", null, PropertyType.STRING);
      document.field("superClasses", null, PropertyType.EMBEDDEDLIST);
    } else {
      // Single super class is deprecated!
      document.field("superClass", superClasses.get(0).getName(), PropertyType.STRING);
      List<String> superClassesNames = new ArrayList<String>();
      for (SchemaClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      document.field("superClasses", superClassesNames, PropertyType.EMBEDDEDLIST);
    }

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        PropertyType.EMBEDDEDMAP);

    return document;
  }

  @Override
  public int getClusterForNewInstance(final EntityImpl doc) {
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
    final Property p = properties.remove(iOldName);
    if (p != null) {
      properties.put(iNewName, p);
    }
  }

  public static SchemaClass addClusters(DatabaseSessionInternal session, final SchemaClass cls,
      final int iClusters) {
    final String clusterBase = cls.getName().toLowerCase(Locale.ENGLISH) + "_";
    for (int i = 0; i < iClusters; ++i) {
      cls.addCluster(session, clusterBase + i);
    }
    return cls;
  }

  protected void truncateClusterInternal(
      final String clusterName, final DatabaseSessionInternal database) {
    database.truncateCluster(clusterName);
  }

  public Collection<SchemaClass> getSubclasses() {
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

  public Collection<SchemaClass> getAllSubclasses() {
    acquireSchemaReadLock();
    try {
      final Set<SchemaClass> set = new HashSet<SchemaClass>();
      if (subclasses != null) {
        set.addAll(subclasses);

        for (SchemaClass c : subclasses) {
          set.addAll(c.getAllSubclasses());
        }
      }
      return set;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Deprecated
  public Collection<SchemaClass> getBaseClasses() {
    return getSubclasses();
  }

  @Deprecated
  public Collection<SchemaClass> getAllBaseClasses() {
    return getAllSubclasses();
  }

  @Override
  public Collection<SchemaClass> getAllSuperClasses() {
    Set<SchemaClass> ret = new HashSet<SchemaClass>();
    getAllSuperClasses(ret);
    return ret;
  }

  private void getAllSuperClasses(Set<SchemaClass> set) {
    set.addAll(superClasses);
    for (SchemaClassImpl superClass : superClasses) {
      superClass.getAllSuperClasses(set);
    }
  }

  public abstract SchemaClass removeBaseClassInternal(DatabaseSessionInternal session,
      final SchemaClass baseClass);

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
      for (SchemaClassImpl superClass : superClasses) {
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
      if (!SchemaClass.class.isAssignableFrom(obj.getClass())) {
        return false;
      }
      final SchemaClass other = (SchemaClass) obj;
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

  public int compareTo(final SchemaClass o) {
    acquireSchemaReadLock();
    try {
      return name.compareTo(o.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public long count(DatabaseSession session) {
    return count(session, true);
  }

  public long count(DatabaseSession session, final boolean isPolymorphic) {
    var sessionInternal = (DatabaseSessionInternal) session;
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
  public void truncate(DatabaseSession session) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.truncateClass(name, false);
  }

  /**
   * Check if the current instance extends specified schema class.
   *
   * @param iClassName of class that should be checked
   * @return Returns true if the current instance extends the passed schema class (iClass)
   * @see #isSuperClassOf(SchemaClass)
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
      for (SchemaClassImpl superClass : superClasses) {
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
   * @see #isSuperClassOf(SchemaClass)
   */
  public boolean isSubClassOf(final SchemaClass clazz) {
    acquireSchemaReadLock();
    try {
      if (clazz == null) {
        return false;
      }
      if (equals(clazz)) {
        return true;
      }
      for (SchemaClassImpl superClass : superClasses) {
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
   * @see #isSubClassOf(SchemaClass)
   */
  public boolean isSuperClassOf(final SchemaClass clazz) {
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

  public SchemaClass set(DatabaseSession session, final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    final String stringValue = iValue != null ? iValue.toString() : null;
    final boolean isNull = stringValue == null || stringValue.equalsIgnoreCase("NULL");

    var sessionInternal = (DatabaseSessionInternal) session;
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

  public abstract SchemaClassImpl setEncryption(DatabaseSessionInternal session,
      final String iValue);

  public Index createIndex(DatabaseSession session, final String iName, final INDEX_TYPE iType,
      final String... fields) {
    return createIndex(session, iName, iType.name(), fields);
  }

  public Index createIndex(DatabaseSession session, final String iName, final String iType,
      final String... fields) {
    return createIndex(session, iName, iType, null, null, fields);
  }

  public Index createIndex(
      DatabaseSession session, final String iName,
      final INDEX_TYPE iType,
      final ProgressListener iProgressListener,
      final String... fields) {
    return createIndex(session, iName, iType.name(), iProgressListener, null, fields);
  }

  public Index createIndex(
      DatabaseSession session, String iName,
      String iType,
      ProgressListener iProgressListener,
      EntityImpl metadata,
      String... fields) {
    return createIndex(session, iName, iType, iProgressListener, metadata, null, fields);
  }

  public Index createIndex(
      DatabaseSession session, final String name,
      String type,
      final ProgressListener progressListener,
      EntityImpl metadata,
      String algorithm,
      final String... fields) {
    if (type == null) {
      throw new IllegalArgumentException("Index type is null");
    }

    type = type.toUpperCase(Locale.ENGLISH);

    if (fields.length == 0) {
      throw new IndexException("List of fields to index cannot be empty.");
    }

    var sessionInternal = (DatabaseSessionInternal) session;
    final String localName = this.name;
    final int[] localPolymorphicClusterIds = polymorphicClusterIds;

    for (final String fieldToIndex : fields) {
      final String fieldName =
          decodeClassName(IndexDefinitionFactory.extractFieldName(fieldToIndex));

      if (!fieldName.equals("@rid") && !existsProperty(fieldName)) {
        throw new IndexException(
            "Index with name '"
                + name
                + "' cannot be created on class '"
                + localName
                + "' because the field '"
                + fieldName
                + "' is absent in class definition");
      }
    }

    final IndexDefinition indexDefinition =
        IndexDefinitionFactory.createIndexDefinition(
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

  public boolean areIndexed(DatabaseSession session, final String... fields) {
    return areIndexed(session, Arrays.asList(fields));
  }

  public boolean areIndexed(DatabaseSession session, final Collection<String> fields) {
    var sessionInternal = (DatabaseSessionInternal) session;
    final IndexManagerAbstract indexManager =
        sessionInternal.getMetadata().getIndexManagerInternal();

    acquireSchemaReadLock();
    try {
      final boolean currentClassResult = indexManager.areIndexed(name, fields);

      if (currentClassResult) {
        return true;
      }
      for (SchemaClassImpl superClass : superClasses) {
        if (superClass.areIndexed(sessionInternal, fields)) {
          return true;
        }
      }
      return false;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<Index> getInvolvedIndexes(DatabaseSession session, final String... fields) {
    return getInvolvedIndexes(session, Arrays.asList(fields));
  }

  public Set<Index> getInvolvedIndexes(DatabaseSession session,
      final Collection<String> fields) {
    acquireSchemaReadLock();
    try {
      final Set<Index> result = new HashSet<Index>(getClassInvolvedIndexes(session, fields));

      for (SchemaClassImpl superClass : superClasses) {
        result.addAll(superClass.getInvolvedIndexes(session, fields));
      }

      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<Index> getClassInvolvedIndexes(DatabaseSession session,
      final Collection<String> fields) {
    final DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    acquireSchemaReadLock();
    try {
      return indexManager.getClassInvolvedIndexes(database, name, fields);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<Index> getClassInvolvedIndexes(DatabaseSession session, final String... fields) {
    return getClassInvolvedIndexes(session, Arrays.asList(fields));
  }

  public Index getClassIndex(DatabaseSession session, final String name) {
    acquireSchemaReadLock();
    try {
      final DatabaseSessionInternal database = (DatabaseSessionInternal) session;
      return database
          .getMetadata()
          .getIndexManagerInternal()
          .getClassIndex(database, this.name, name);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<Index> getClassIndexes(DatabaseSession session) {
    acquireSchemaReadLock();
    try {
      final DatabaseSessionInternal database = (DatabaseSessionInternal) session;
      final IndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
      if (idxManager == null) {
        return new HashSet<>();
      }

      return idxManager.getClassIndexes(database, name);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public void getClassIndexes(DatabaseSession session, final Collection<Index> indexes) {
    acquireSchemaReadLock();
    try {
      final DatabaseSessionInternal database = (DatabaseSessionInternal) session;
      final IndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
      if (idxManager == null) {
        return;
      }

      idxManager.getClassIndexes(database, name, indexes);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public Index getAutoShardingIndex(DatabaseSession session) {
    final DatabaseSessionInternal db = (DatabaseSessionInternal) session;
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

  public void onPostIndexManagement(DatabaseSessionInternal session) {
    final Index autoShardingIndex = getAutoShardingIndex(session);
    if (autoShardingIndex != null) {
      if (!session.isRemote()) {
        // OVERRIDE CLUSTER SELECTION
        acquireSchemaWriteLock(session);
        try {
          this.clusterSelection =
              new AutoShardingClusterSelectionStrategy(this, autoShardingIndex);
        } finally {
          releaseSchemaWriteLock(session);
        }
      }
    } else if (clusterSelection instanceof AutoShardingClusterSelectionStrategy) {
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
  public void getIndexes(DatabaseSession session, final Collection<Index> indexes) {
    acquireSchemaReadLock();
    try {
      getClassIndexes(session, indexes);
      for (SchemaClass superClass : superClasses) {
        superClass.getIndexes(session, indexes);
      }
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<Index> getIndexes(DatabaseSession session) {
    final Set<Index> indexes = new HashSet<Index>();
    getIndexes(session, indexes);
    return indexes;
  }

  public void acquireSchemaReadLock() {
    owner.acquireSchemaReadLock();
  }

  public void releaseSchemaReadLock() {
    owner.releaseSchemaReadLock();
  }

  public void acquireSchemaWriteLock(DatabaseSessionInternal session) {
    owner.acquireSchemaWriteLock(session);
  }

  public void releaseSchemaWriteLock(DatabaseSessionInternal session) {
    releaseSchemaWriteLock(session, true);
  }

  public void releaseSchemaWriteLock(DatabaseSessionInternal session, final boolean iSave) {
    calculateHashCode();
    owner.releaseSchemaWriteLock(session, iSave);
  }

  public void checkEmbedded() {
    owner.checkEmbedded();
  }

  public void setClusterSelectionInternal(final ClusterSelectionStrategy iClusterSelection) {
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
      final DatabaseSession database, final String propertyName, final PropertyType type) {
    final boolean strictSQL =
        ((DatabaseSessionInternal) database).getStorageInfo().getConfiguration().isStrictSql();

    try (ResultSet result =
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
              EntityImpl record =
                  database.bindToSession((EntityImpl) result.next().getEntity().get());
              record.field(propertyName, record.field(propertyName), type);
              database.save(record);
            });
      }
    }
  }

  public void firePropertyNameMigration(
      final DatabaseSession database,
      final String propertyName,
      final String newPropertyName,
      final PropertyType type) {
    final boolean strictSQL =
        ((DatabaseSessionInternal) database).getStorageInfo().getConfiguration().isStrictSql();

    try (ResultSet result =
        database.query(
            "select from "
                + getEscapedName(name, strictSQL)
                + " where "
                + getEscapedName(propertyName, strictSQL)
                + " is not null ")) {
      while (result.hasNext()) {
        database.executeInTx(
            () -> {
              EntityImpl record =
                  database.bindToSession((EntityImpl) result.next().getEntity().get());
              record.setFieldType(propertyName, type);
              record.field(newPropertyName, record.field(propertyName), type);
              database.save(record);
            });
      }
    }
  }

  public void checkPersistentPropertyType(
      final DatabaseSessionInternal database,
      final String propertyName,
      final PropertyType type,
      SchemaClass linkedClass) {
    if (PropertyType.ANY.equals(type)) {
      return;
    }
    final boolean strictSQL = database.getStorageInfo().getConfiguration().isStrictSql();

    final StringBuilder builder = new StringBuilder(256);
    builder.append("select from ");
    builder.append(getEscapedName(name, strictSQL));
    builder.append(" where ");
    builder.append(getEscapedName(propertyName, strictSQL));
    builder.append(".type() not in [");

    final Iterator<PropertyType> cur = type.getCastable().iterator();
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

    try (final ResultSet res = database.command(builder.toString())) {
      if (res.hasNext()) {
        throw new SchemaException(
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
      DatabaseSessionInternal database, String propertyName, PropertyType type,
      SchemaClass linkedClass) {
    final StringBuilder builder = new StringBuilder(256);
    builder.append("select from ");
    builder.append(getEscapedName(name, true));
    builder.append(" where ");
    builder.append(getEscapedName(propertyName, true)).append(" is not null ");
    if (type.isMultiValue()) {
      builder.append(" and ").append(getEscapedName(propertyName, true)).append(".size() > 0");
    }

    try (final ResultSet res = database.command(builder.toString())) {
      while (res.hasNext()) {
        Result item = res.next();
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
                        throw new SchemaException(
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
            } catch (SchemaException e1) {
              throw e1;
            } catch (Exception e) {
            }
            break;
          case EMBEDDED:
          case LINK:
            Object elem = item.getProperty(propertyName);
            if (!matchesType(elem, linkedClass)) {
              throw new SchemaException(
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

  protected static boolean matchesType(Object x, SchemaClass linkedClass) {
    if (x instanceof Result) {
      x = ((Result) x).toEntity();
    }
    if (x instanceof RID) {
      try {
        x = ((RID) x).getRecord();
      } catch (RecordNotFoundException e) {
        return true;
      }
    }
    if (x == null) {
      return true;
    }
    if (!(x instanceof Entity)) {
      return false;
    }
    return !(x instanceof EntityImpl)
        || linkedClass.getName().equalsIgnoreCase(((EntityImpl) x).getClassName());
  }

  protected static String getEscapedName(final String iName, final boolean iStrictSQL) {
    if (iStrictSQL)
    // ESCAPE NAME
    {
      return "`" + iName + "`";
    }
    return iName;
  }

  public SchemaShared getOwner() {
    return owner;
  }

  private void calculateHashCode() {
    int result = super.hashCode();
    result = 31 * result + (name != null ? name.hashCode() : 0);
    hashCode = result;
  }

  protected void renameCluster(DatabaseSessionInternal session, String oldName, String newName) {
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

    polymorphicClusterIds = ArrayUtils.copyOf(polymorphicClusterIds,
        polymorphicClusterIds.length + 1);
    polymorphicClusterIds[polymorphicClusterIds.length - 1] = clusterId;
    Arrays.sort(polymorphicClusterIds);

    for (SchemaClassImpl superClass : superClasses) {
      superClass.onlyAddPolymorphicClusterId(clusterId);
    }
  }

  protected abstract Property addProperty(
      DatabaseSessionInternal session, final String propertyName,
      final PropertyType type,
      final PropertyType linkedType,
      final SchemaClass linkedClass,
      final boolean unsafe);

  protected void validatePropertyName(final String propertyName) {
  }

  protected abstract void addClusterIdToIndexes(DatabaseSessionInternal session, int iId);

  /**
   * Adds a base class to the current one. It adds also the base class cluster ids to the
   * polymorphic cluster ids array.
   *
   * @param session
   * @param iBaseClass The base class to add.
   */
  public SchemaClass addBaseClass(DatabaseSessionInternal session,
      final SchemaClassImpl iBaseClass) {
    checkRecursion(iBaseClass);

    if (subclasses == null) {
      subclasses = new ArrayList<SchemaClass>();
    }

    if (subclasses.contains(iBaseClass)) {
      return this;
    }

    subclasses.add(iBaseClass);
    addPolymorphicClusterIdsWithInheritance(session, iBaseClass);
    return this;
  }

  protected void checkParametersConflict(DatabaseSessionInternal session,
      final SchemaClass baseClass) {
    final Collection<Property> baseClassProperties = baseClass.properties(session);
    for (Property property : baseClassProperties) {
      Property thisProperty = getProperty(property.getName());
      if (thisProperty != null && !thisProperty.getType().equals(property.getType())) {
        throw new SchemaException(
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

  public static void checkParametersConflict(List<SchemaClass> classes) {
    final Map<String, Property> comulative = new HashMap<String, Property>();
    final Map<String, Property> properties = new HashMap<String, Property>();

    for (SchemaClass superClass : classes) {
      if (superClass == null) {
        continue;
      }
      SchemaClassImpl impl;

      if (superClass instanceof SchemaClassAbstractDelegate) {
        impl = (SchemaClassImpl) ((SchemaClassAbstractDelegate) superClass).delegate;
      } else {
        impl = (SchemaClassImpl) superClass;
      }
      impl.propertiesMap(properties);
      for (Map.Entry<String, Property> entry : properties.entrySet()) {
        if (comulative.containsKey(entry.getKey())) {
          final String property = entry.getKey();
          final Property existingProperty = comulative.get(property);
          if (!existingProperty.getType().equals(entry.getValue().getType())) {
            throw new SchemaException(
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

  private void checkRecursion(final SchemaClass baseClass) {
    if (isSubClassOf(baseClass)) {
      throw new SchemaException(
          "Cannot add base class '" + baseClass.getName() + "', because of recursion");
    }
  }

  protected void removePolymorphicClusterIds(DatabaseSessionInternal session,
      final SchemaClassImpl iBaseClass) {
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

    for (SchemaClassImpl superClass : superClasses) {
      superClass.onlyRemovePolymorphicClusterId(clusterId);
    }
  }

  protected void removePolymorphicClusterId(DatabaseSessionInternal session,
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
    for (SchemaClassImpl superClass : superClasses) {
      superClass.removePolymorphicClusterId(session, clusterId);
    }
  }

  private void removeClusterFromIndexes(DatabaseSessionInternal session, final int iId) {
    if (session.getStorage() instanceof AbstractPaginatedStorage) {
      final String clusterName = session.getClusterNameById(iId);
      final List<String> indexesToRemove = new ArrayList<String>();

      for (final Index index : getIndexes(session)) {
        indexesToRemove.add(index.getName());
      }

      final IndexManagerAbstract indexManager =
          session.getMetadata().getIndexManagerInternal();
      for (final String indexName : indexesToRemove) {
        indexManager.removeClusterFromIndex(session, clusterName, indexName);
      }
    }
  }

  /**
   * Add different cluster id to the "polymorphic cluster ids" array.
   */
  protected void addPolymorphicClusterIds(DatabaseSessionInternal session,
      final SchemaClassImpl iBaseClass) {
    IntRBTreeSet clusters = new IntRBTreeSet(polymorphicClusterIds);

    for (int clusterId : iBaseClass.polymorphicClusterIds) {
      if (clusters.add(clusterId)) {
        try {
          addClusterIdToIndexes(session, clusterId);
        } catch (RuntimeException e) {
          LogManager.instance()
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

  private void addPolymorphicClusterIdsWithInheritance(DatabaseSessionInternal session,
      final SchemaClassImpl iBaseClass) {
    addPolymorphicClusterIds(session, iBaseClass);
    for (SchemaClassImpl superClass : superClasses) {
      superClass.addPolymorphicClusterIdsWithInheritance(session, iBaseClass);
    }
  }

  public List<PropertyType> extractFieldTypes(final String[] fieldNames) {
    final List<PropertyType> types = new ArrayList<PropertyType>(fieldNames.length);

    for (String fieldName : fieldNames) {
      if (!fieldName.equals("@rid")) {
        types.add(
            getProperty(decodeClassName(IndexDefinitionFactory.extractFieldName(fieldName)))
                .getType());
      } else {
        types.add(PropertyType.LINK);
      }
    }
    return types;
  }

  protected SchemaClass setClusterIds(final int[] iClusterIds) {
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

  public EntityImpl toNetworkStream() {
    EntityImpl document = new EntityImpl();
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

    final Set<EntityImpl> props = new LinkedHashSet<EntityImpl>();
    for (final Property p : properties.values()) {
      props.add(((PropertyImpl) p).toNetworkStream());
    }
    document.field("properties", props, PropertyType.EMBEDDEDSET);

    if (superClasses.isEmpty()) {
      // Single super class is deprecated!
      document.field("superClass", null, PropertyType.STRING);
      document.field("superClasses", null, PropertyType.EMBEDDEDLIST);
    } else {
      // Single super class is deprecated!
      document.field("superClass", superClasses.get(0).getName(), PropertyType.STRING);
      List<String> superClassesNames = new ArrayList<String>();
      for (SchemaClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      document.field("superClasses", superClassesNames, PropertyType.EMBEDDEDLIST);
    }

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        PropertyType.EMBEDDEDMAP);

    return document;
  }
}
