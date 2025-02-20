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
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.exception.SecurityAccessException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.ArrayUtils;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinitionFactory;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.MaterializedEntity;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.MaterializedEntityMetadataProcessor;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sharding.auto.AutoShardingClusterSelectionStrategy;
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
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Schema Class implementation.
 */
@SuppressWarnings("unchecked")
public abstract class SchemaClassImpl implements SchemaClassInternal {

  protected static final int NOT_EXISTENT_CLUSTER_ID = -1;
  protected final SchemaShared owner;
  protected final Map<String, SchemaPropertyInternal> properties = new HashMap<>();
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
  @Nullable
  protected Class<? extends MaterializedEntity> materializedEntityInterface;

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

  public void setMaterializedEntity(
      @Nonnull Class<? extends MaterializedEntity> materializedEntityInterface) {
    MaterializedEntityMetadataProcessor.validateAndFetchMaterializedEntityAccessMethods(
        materializedEntityInterface);
    if (this.materializedEntityInterface != null) {
      if (!this.materializedEntityInterface
          .equals(materializedEntityInterface)) {
        throw new SchemaException("Materialized entity class is already registered");
      }
      return;
    }

    this.materializedEntityInterface = materializedEntityInterface;
  }

  @Nullable
  @Override
  public Class<? extends MaterializedEntity> getMaterializedEntity() {
    return materializedEntityInterface;
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
  public String getClusterSelectionStrategyName() {
    return getClusterSelection().getName();
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

  public Collection<SchemaProperty> declaredProperties() {
    acquireSchemaReadLock();
    try {
      return Collections.unmodifiableCollection(properties.values());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Map<String, SchemaProperty> propertiesMap(DatabaseSession session) {
    ((DatabaseSessionInternal) session).checkSecurity(Rule.ResourceGeneric.SCHEMA,
        Role.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final Map<String, SchemaProperty> props = new HashMap<String, SchemaProperty>(20);
      propertiesMap(props);
      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void propertiesMap(Map<String, SchemaProperty> propertiesMap) {
    for (SchemaProperty p : properties.values()) {
      String propName = p.getName();
      if (!propertiesMap.containsKey(propName)) {
        propertiesMap.put(propName, p);
      }
    }
    for (SchemaClassImpl superClass : superClasses) {
      superClass.propertiesMap(propertiesMap);
    }
  }

  public Collection<SchemaProperty> properties(DatabaseSession session) {
    ((DatabaseSessionInternal) session).checkSecurity(Rule.ResourceGeneric.SCHEMA,
        Role.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      final Collection<SchemaProperty> props = new ArrayList<SchemaProperty>();
      properties(props);
      return props;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void properties(Collection<SchemaProperty> properties) {
    properties.addAll(this.properties.values());
    for (SchemaClassImpl superClass : superClasses) {
      superClass.properties(properties);
    }
  }

  public void getIndexedProperties(DatabaseSessionInternal session,
      Collection<SchemaProperty> indexedProperties) {
    for (SchemaProperty p : properties.values()) {
      if (areIndexed(session, p.getName())) {
        indexedProperties.add(p);
      }
    }
    for (SchemaClassImpl superClass : superClasses) {
      superClass.getIndexedProperties(session, indexedProperties);
    }
  }

  @Override
  public Collection<SchemaProperty> getIndexedProperties(DatabaseSession session) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA,
        Role.PERMISSION_READ);

    acquireSchemaReadLock();
    try {
      Collection<SchemaProperty> indexedProps = new HashSet<SchemaProperty>();
      getIndexedProperties(sessionInternal, indexedProps);
      return indexedProps;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public SchemaProperty getProperty(String propertyName) {
    return getPropertyInternal(propertyName);
  }

  @Override
  public SchemaPropertyInternal getPropertyInternal(String propertyName) {
    acquireSchemaReadLock();
    try {
      var p = properties.get(propertyName);

      if (p != null) {
        return p;
      }

      for (int i = 0; i < superClasses.size() && p == null; i++) {
        p = superClasses.get(i).getPropertyInternal(propertyName);
      }

      return p;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public SchemaProperty createProperty(DatabaseSession session, final String iPropertyName,
      final PropertyType iType) {
    return addProperty((DatabaseSessionInternal) session, iPropertyName, iType, null, null,
        false);
  }

  public SchemaProperty createProperty(
      DatabaseSession session, final String iPropertyName, final PropertyType iType,
      final SchemaClass iLinkedClass) {
    return addProperty((DatabaseSessionInternal) session, iPropertyName, iType, null,
        iLinkedClass,
        false);
  }

  public SchemaProperty createProperty(
      DatabaseSession session, final String iPropertyName,
      final PropertyType iType,
      final SchemaClass iLinkedClass,
      final boolean unsafe) {
    return addProperty((DatabaseSessionInternal) session, iPropertyName, iType, null,
        iLinkedClass,
        unsafe);
  }

  public SchemaProperty createProperty(
      DatabaseSession session, final String iPropertyName, final PropertyType iType,
      final PropertyType iLinkedType) {
    return addProperty((DatabaseSessionInternal) session, iPropertyName, iType, iLinkedType, null,
        false);
  }

  public SchemaProperty createProperty(
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

  public void fromStream(EntityImpl entity) {
    subclasses = null;
    superClasses.clear();

    name = entity.field("name");
    if (entity.containsField("shortName")) {
      shortName = entity.field("shortName");
    } else {
      shortName = null;
    }
    if (entity.containsField("description")) {
      description = entity.field("description");
    } else {
      description = null;
    }
    defaultClusterId = entity.field("defaultClusterId");
    if (entity.containsField("strictMode")) {
      strictMode = entity.field("strictMode");
    } else {
      strictMode = false;
    }

    if (entity.containsField("abstract")) {
      abstractClass = entity.field("abstract");
    } else {
      abstractClass = false;
    }

    if (entity.field("overSize") != null) {
      overSize = entity.field("overSize");
    } else {
      overSize = 0f;
    }

    final Object cc = entity.field("clusterIds");
    if (cc instanceof Collection<?>) {
      final Collection<Integer> coll = entity.field("clusterIds");
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
    SchemaPropertyImpl prop;

    final Map<String, SchemaPropertyInternal> newProperties = new HashMap<>();
    final Collection<EntityImpl> storedProperties = entity.field("properties");

    if (storedProperties != null) {
      for (Identifiable id : storedProperties) {
        EntityImpl p = id.getRecord();
        String name = p.field("name");
        // To lower case ?
        if (properties.containsKey(name)) {
          prop = (SchemaPropertyImpl) properties.get(name);
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
    customFields = entity.field("customFields", PropertyType.EMBEDDEDMAP);
    clusterSelection =
        owner.getClusterSelectionFactory().getStrategy(entity.field("clusterSelection"));
  }

  protected abstract SchemaPropertyImpl createPropertyInstance();

  public EntityImpl toStream() {
    EntityImpl entity = new EntityImpl();

    entity.field("name", name);
    entity.field("shortName", shortName);
    entity.field("description", description);
    entity.field("defaultClusterId", defaultClusterId);
    entity.field("clusterIds", clusterIds);
    entity.field("clusterSelection", clusterSelection.getName());
    entity.field("overSize", overSize);
    entity.field("strictMode", strictMode);
    entity.field("abstract", abstractClass);

    entity.setPropertyInternal("materializedEntityInterface",
        materializedEntityInterface != null ? materializedEntityInterface.getName() : null);

    final Set<EntityImpl> props = new LinkedHashSet<EntityImpl>();
    for (final SchemaProperty p : properties.values()) {
      props.add(((SchemaPropertyImpl) p).toStream());
    }
    entity.field("properties", props, PropertyType.EMBEDDEDSET);

    if (superClasses.isEmpty()) {
      // Single super class is deprecated!
      entity.field("superClass", null, PropertyType.STRING);
      entity.field("superClasses", null, PropertyType.EMBEDDEDLIST);
    } else {
      // Single super class is deprecated!
      entity.field("superClass", superClasses.get(0).getName(), PropertyType.STRING);
      List<String> superClassesNames = new ArrayList<String>();
      for (SchemaClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      entity.field("superClasses", superClassesNames, PropertyType.EMBEDDEDLIST);
    }

    entity.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        PropertyType.EMBEDDEDMAP);

    return entity;
  }

  @Override
  public int getClusterForNewInstance(final EntityImpl entity) {
    acquireSchemaReadLock();
    try {
      return clusterSelection.getCluster(this, entity);
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
    var p = properties.remove(iOldName);
    if (p != null) {
      properties.put(iNewName, p);
    }
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
      case STRICT_MODE:
        return isStrictMode();
      case ABSTRACT:
        return isAbstract();
      case CLUSTER_SELECTION:
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
      case STRICT_MODE:
        setStrictMode(session, Boolean.parseBoolean(stringValue));
        break;
      case ABSTRACT:
        setAbstract(session, Boolean.parseBoolean(stringValue));
        break;
      case ADD_CLUSTER: {
        addCluster(session, stringValue);
        break;
      }
      case REMOVE_CLUSTER:
        int clId = owner.getClusterId(sessionInternal, stringValue);
        if (clId == NOT_EXISTENT_CLUSTER_ID) {
          throw new IllegalArgumentException("Cluster id '" + stringValue + "' cannot be removed");
        }
        removeClusterId(session, clId);
        break;
      case CLUSTER_SELECTION:
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

  public void createIndex(DatabaseSession session, final String iName, final INDEX_TYPE iType,
      final String... fields) {
    createIndex(session, iName, iType.name(), fields);
  }

  public void createIndex(DatabaseSession session, final String iName, final String iType,
      final String... fields) {
    createIndex(session, iName, iType, null, null, fields);
  }

  public void createIndex(
      DatabaseSession session, final String iName,
      final INDEX_TYPE iType,
      final ProgressListener iProgressListener,
      final String... fields) {
    createIndex(session, iName, iType.name(), iProgressListener, null, fields);
  }

  public void createIndex(
      DatabaseSession session, String iName,
      String iType,
      ProgressListener iProgressListener,
      Map<String, ?> metadata,
      String... fields) {
    createIndex(session, iName, iType, iProgressListener, metadata, null, fields);
  }

  public void createIndex(
      DatabaseSession session, final String name,
      String type,
      final ProgressListener progressListener,
      Map<String, ?> metadata,
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

    sessionInternal
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

  @Override
  public Set<String> getInvolvedIndexes(DatabaseSession session, final String... fields) {
    return getInvolvedIndexes(session, Arrays.asList(fields));
  }

  @Override
  public Set<Index> getInvolvedIndexesInternal(DatabaseSession session, String... fields) {
    return getInvolvedIndexesInternal(session, Arrays.asList(fields));
  }

  @Override
  public Set<String> getInvolvedIndexes(DatabaseSession session,
      final Collection<String> fields) {
    return getInvolvedIndexesInternal(session, fields).stream().map(Index::getName)
        .collect(Collectors.toSet());
  }

  @Override
  public Set<Index> getInvolvedIndexesInternal(DatabaseSession session,
      Collection<String> fields) {
    acquireSchemaReadLock();
    try {
      final Set<Index> result = new HashSet<>(getClassInvolvedIndexesInternal(session, fields));

      for (SchemaClassImpl superClass : superClasses) {
        result.addAll(superClass.getInvolvedIndexesInternal(session, fields));
      }

      return result;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public Set<String> getClassInvolvedIndexes(DatabaseSession session,
      final Collection<String> fields) {
    return getClassInvolvedIndexesInternal(session, fields).stream().map(Index::getName)
        .collect(Collectors.toSet());
  }

  @Override
  public Set<Index> getClassInvolvedIndexesInternal(DatabaseSession session,
      Collection<String> fields) {
    final DatabaseSessionInternal database = (DatabaseSessionInternal) session;
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    acquireSchemaReadLock();
    try {
      return indexManager.getClassInvolvedIndexes(database, name, fields);
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public Set<String> getClassInvolvedIndexes(DatabaseSession session, final String... fields) {
    return getClassInvolvedIndexes(session, Arrays.asList(fields));
  }

  @Override
  public Set<Index> getClassInvolvedIndexesInternal(DatabaseSession session, String... fields) {
    return getClassInvolvedIndexesInternal(session, Arrays.asList(fields));
  }

  @Override
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

  @Override
  public Set<String> getClassIndexes(DatabaseSession session) {
    return getClassInvolvedIndexesInternal(session).stream().map(Index::getName)
        .collect(Collectors.toSet());
  }

  @Override
  public Set<Index> getClassIndexesInternal(DatabaseSession session) {
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
  public boolean isEdgeType() {
    return isSubClassOf(EDGE_CLASS_NAME);
  }

  @Override
  public boolean isVertexType() {
    return isSubClassOf(VERTEX_CLASS_NAME);
  }

  public void onPostIndexManagement(DatabaseSessionInternal session) {
    if (clusterSelection instanceof AutoShardingClusterSelectionStrategy) {
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
  public void getIndexesInternal(DatabaseSession session, final Collection<Index> indexes) {
    acquireSchemaReadLock();
    try {
      getClassIndexes(session, indexes);
      for (var superClass : superClasses) {
        superClass.getIndexesInternal(session, indexes);
      }
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Set<String> getIndexes(DatabaseSession session) {
    return getIndexesInternal(session).stream().map(Index::getName).collect(Collectors.toSet());
  }

  @Override
  public Set<Index> getIndexesInternal(DatabaseSession session) {
    final Set<Index> indexes = new HashSet<Index>();
    getIndexesInternal(session, indexes);

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

  protected abstract SchemaProperty addProperty(
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
    final Collection<SchemaProperty> baseClassProperties = baseClass.properties(session);
    for (SchemaProperty property : baseClassProperties) {
      SchemaProperty thisProperty = getProperty(property.getName());
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
    final Map<String, SchemaProperty> comulative = new HashMap<String, SchemaProperty>();
    final Map<String, SchemaProperty> properties = new HashMap<String, SchemaProperty>();

    for (SchemaClass superClass : classes) {
      if (superClass == null) {
        continue;
      }
      SchemaClassImpl impl;
      impl = (SchemaClassImpl) superClass;
      impl.propertiesMap(properties);
      for (Map.Entry<String, SchemaProperty> entry : properties.entrySet()) {
        if (comulative.containsKey(entry.getKey())) {
          final String property = entry.getKey();
          final SchemaProperty existingProperty = comulative.get(property);
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

      final Set<Index> indexes = new HashSet<Index>();
      getIndexesInternal(session, indexes);

      for (final Index index : indexes) {
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
    EntityImpl entity = new EntityImpl();
    entity.setTrackingChanges(false);
    entity.field("name", name);
    entity.field("shortName", shortName);
    entity.field("description", description);
    entity.field("defaultClusterId", defaultClusterId);
    entity.field("clusterIds", clusterIds);
    entity.field("clusterSelection", clusterSelection.getName());
    entity.field("overSize", overSize);
    entity.field("strictMode", strictMode);
    entity.field("abstract", abstractClass);

    final Set<EntityImpl> props = new LinkedHashSet<EntityImpl>();
    for (final SchemaProperty p : properties.values()) {
      props.add(((SchemaPropertyImpl) p).toNetworkStream());
    }
    entity.field("properties", props, PropertyType.EMBEDDEDSET);

    if (superClasses.isEmpty()) {
      // Single super class is deprecated!
      entity.field("superClass", null, PropertyType.STRING);
      entity.field("superClasses", null, PropertyType.EMBEDDEDLIST);
    } else {
      // Single super class is deprecated!
      entity.field("superClass", superClasses.get(0).getName(), PropertyType.STRING);
      List<String> superClassesNames = new ArrayList<String>();
      for (SchemaClassImpl superClass : superClasses) {
        superClassesNames.add(superClass.getName());
      }
      entity.field("superClasses", superClassesNames, PropertyType.EMBEDDEDLIST);
    }

    entity.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        PropertyType.EMBEDDEDMAP);

    return entity;
  }
}
