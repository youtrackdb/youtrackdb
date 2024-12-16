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

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.RecordHook;
import com.jetbrains.youtrack.db.api.schema.GlobalProperty;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.util.ArrayUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection.ClusterSelectionFactory;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * @since 10/21/14
 */
public class ImmutableSchema implements SchemaInternal {

  private final Int2ObjectOpenHashMap<SchemaClass> clustersToClasses;
  private final Map<String, SchemaClassInternal> classes;
  private final IntSet blogClusters;

  public final int version;
  private final RID identity;
  private final List<GlobalProperty> properties;
  private final ClusterSelectionFactory clusterSelectionFactory;

  public ImmutableSchema(SchemaShared schemaShared, DatabaseSessionInternal database) {
    version = schemaShared.getVersion();
    identity = schemaShared.getIdentity();
    clusterSelectionFactory = schemaShared.getClusterSelectionFactory();

    clustersToClasses = new Int2ObjectOpenHashMap<>(schemaShared.getClasses(database).size() * 3);
    classes = new HashMap<>(schemaShared.getClasses(database).size());

    for (SchemaClass oClass : schemaShared.getClasses(database)) {
      final SchemaImmutableClass immutableClass = new SchemaImmutableClass(database,
          (SchemaClassInternal) oClass, this);

      classes.put(immutableClass.getName().toLowerCase(Locale.ENGLISH), immutableClass);
      if (immutableClass.getShortName() != null) {
        classes.put(immutableClass.getShortName().toLowerCase(Locale.ENGLISH), immutableClass);
      }

      for (int clusterId : immutableClass.getClusterIds()) {
        clustersToClasses.put(clusterId, immutableClass);
      }
    }

    properties = new ArrayList<GlobalProperty>();
    for (GlobalProperty globalProperty : schemaShared.getGlobalProperties()) {
      properties.add(globalProperty);
    }

    for (SchemaClass cl : classes.values()) {
      ((SchemaImmutableClass) cl).init();
    }
    this.blogClusters = schemaShared.getBlobClusters();
  }

  public ImmutableSchema makeSnapshot() {
    return this;
  }

  @Override
  public int countClasses() {
    return classes.size();
  }

  @Override
  public SchemaClass createClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass createClass(String iClassName, SchemaClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass createClass(String iClassName, SchemaClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass createClass(String iClassName, SchemaClass iSuperClass, int[] iClusterIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass createClass(String className, int clusters, SchemaClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass createClass(String className, int[] clusterIds, SchemaClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass createAbstractClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass createAbstractClass(String iClassName, SchemaClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass createAbstractClass(String iClassName, SchemaClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropClass(String iClassName) {
    throw new UnsupportedOperationException();
  }


  @Override
  public boolean existsClass(String iClassName) {
    return classes.containsKey(iClassName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public SchemaClass getClass(Class<?> iClass) {
    if (iClass == null) {
      return null;
    }

    return getClass(iClass.getSimpleName());
  }

  @Override
  public SchemaClass getClass(String iClassName) {
    return getClassInternal(iClassName);
  }

  @Override
  public SchemaClassInternal getClassInternal(String iClassName) {
    if (iClassName == null) {
      return null;
    }

    return classes.get(iClassName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public SchemaClass getOrCreateClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass getOrCreateClass(String iClassName, SchemaClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass getOrCreateClass(String iClassName, SchemaClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<SchemaClass> getClasses() {
    getDatabase().checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_READ);
    return new HashSet<SchemaClass>(classes.values());
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public RecordId getIdentity() {
    return new RecordId(identity);
  }

  public Set<SchemaClass> getClassesRelyOnCluster(String clusterName) {
    getDatabase().checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_READ);

    final int clusterId = getDatabase().getClusterIdByName(clusterName);
    final Set<SchemaClass> result = new HashSet<SchemaClass>();
    for (SchemaClass c : classes.values()) {
      if (ArrayUtils.contains(c.getPolymorphicClusterIds(), clusterId)) {
        result.add(c);
      }
    }

    return result;
  }

  @Override
  public ClusterSelectionFactory getClusterSelectionFactory() {
    return clusterSelectionFactory;
  }

  @Override
  public SchemaClass getClassByClusterId(int clusterId) {
    return clustersToClasses.get(clusterId);
  }


  @Override
  public GlobalProperty getGlobalPropertyById(int id) {
    if (id >= properties.size()) {
      return null;
    }
    return properties.get(id);
  }

  @Override
  public List<GlobalProperty> getGlobalProperties() {
    return Collections.unmodifiableList(properties);
  }

  @Override
  public GlobalProperty createGlobalProperty(String name, PropertyType type, Integer id) {
    throw new UnsupportedOperationException();
  }

  private DatabaseSessionInternal getDatabase() {
    return DatabaseRecordThreadLocal.instance().get();
  }

  public IntSet getBlobClusters() {
    return blogClusters;
  }
}
