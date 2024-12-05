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

import com.jetbrains.youtrack.db.internal.common.util.OArrays;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewCreationListener;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule;
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
public class YTImmutableSchema implements YTSchema {

  private final Int2ObjectOpenHashMap<YTClass> clustersToClasses;
  private final Map<String, YTClass> classes;
  private final IntSet blogClusters;

  private final Int2ObjectOpenHashMap<YTView> clustersToViews;
  private final Map<String, YTView> views;

  public final int version;
  private final YTRID identity;
  private final List<OGlobalProperty> properties;
  private final OClusterSelectionFactory clusterSelectionFactory;

  public YTImmutableSchema(OSchemaShared schemaShared, YTDatabaseSessionInternal database) {
    version = schemaShared.getVersion();
    identity = schemaShared.getIdentity();
    clusterSelectionFactory = schemaShared.getClusterSelectionFactory();

    clustersToClasses = new Int2ObjectOpenHashMap<>(schemaShared.getClasses(database).size() * 3);
    classes = new HashMap<>(schemaShared.getClasses(database).size());

    for (YTClass oClass : schemaShared.getClasses(database)) {
      final YTImmutableClass immutableClass = new YTImmutableClass(database, oClass, this);

      classes.put(immutableClass.getName().toLowerCase(Locale.ENGLISH), immutableClass);
      if (immutableClass.getShortName() != null) {
        classes.put(immutableClass.getShortName().toLowerCase(Locale.ENGLISH), immutableClass);
      }

      for (int clusterId : immutableClass.getClusterIds()) {
        clustersToClasses.put(clusterId, immutableClass);
      }
    }

    properties = new ArrayList<OGlobalProperty>();
    for (OGlobalProperty globalProperty : schemaShared.getGlobalProperties()) {
      properties.add(globalProperty);
    }

    for (YTClass cl : classes.values()) {
      ((YTImmutableClass) cl).init();
    }
    this.blogClusters = schemaShared.getBlobClusters();

    clustersToViews = new Int2ObjectOpenHashMap<>(schemaShared.getViews(database).size() * 3);
    views = new HashMap<String, YTView>(schemaShared.getViews(database).size());

    for (YTView oClass : schemaShared.getViews(database)) {
      final YTImmutableView immutableClass = new YTImmutableView(database, oClass, this);

      views.put(immutableClass.getName().toLowerCase(Locale.ENGLISH), immutableClass);
      if (immutableClass.getShortName() != null) {
        views.put(immutableClass.getShortName().toLowerCase(Locale.ENGLISH), immutableClass);
      }

      for (int clusterId : immutableClass.getClusterIds()) {
        clustersToViews.put(clusterId, immutableClass);
      }
    }
    for (YTClass cl : views.values()) {
      ((YTImmutableClass) cl).init();
    }
  }

  @Override
  public YTImmutableSchema makeSnapshot() {
    return this;
  }

  @Override
  public int countClasses() {
    return classes.size();
  }

  @Override
  public int countViews() {
    return views.size();
  }

  @Override
  public YTClass createClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass createClass(String iClassName, YTClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass createClass(String iClassName, YTClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass createClass(String iClassName, YTClass iSuperClass, int[] iClusterIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass createClass(String className, int clusters, YTClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass createClass(String className, int[] clusterIds, YTClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass createAbstractClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass createAbstractClass(String iClassName, YTClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass createAbstractClass(String iClassName, YTClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTSchema reload() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsClass(String iClassName) {
    return classes.containsKey(iClassName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public YTClass getClass(Class<?> iClass) {
    if (iClass == null) {
      return null;
    }

    return getClass(iClass.getSimpleName());
  }

  @Override
  public YTClass getClass(String iClassName) {
    if (iClassName == null) {
      return null;
    }

    YTClass cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
    return cls;
  }

  @Override
  public YTClass getOrCreateClass(String iClassName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass getOrCreateClass(String iClassName, YTClass iSuperClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass getOrCreateClass(String iClassName, YTClass... superClasses) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<YTClass> getClasses() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);
    return new HashSet<YTClass>(classes.values());
  }

  @Override
  public Collection<YTView> getViews() {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);
    return new HashSet<YTView>(views.values());
  }

  @Override
  public void create() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public YTRID getIdentity() {
    return new YTRecordId(identity);
  }

  @Override
  public Set<YTClass> getClassesRelyOnCluster(String clusterName) {
    getDatabase().checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_READ);

    final int clusterId = getDatabase().getClusterIdByName(clusterName);
    final Set<YTClass> result = new HashSet<YTClass>();
    for (YTClass c : classes.values()) {
      if (OArrays.contains(c.getPolymorphicClusterIds(), clusterId)) {
        result.add(c);
      }
    }

    return result;
  }

  @Override
  public YTClass getClassByClusterId(int clusterId) {
    return clustersToClasses.get(clusterId);
  }

  @Override
  public YTView getViewByClusterId(int clusterId) {
    return clustersToViews.get(clusterId);
  }

  @Override
  public OGlobalProperty getGlobalPropertyById(int id) {
    if (id >= properties.size()) {
      return null;
    }
    return properties.get(id);
  }

  @Override
  public List<OGlobalProperty> getGlobalProperties() {
    return Collections.unmodifiableList(properties);
  }

  @Override
  public OGlobalProperty createGlobalProperty(String name, YTType type, Integer id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClusterSelectionFactory getClusterSelectionFactory() {
    return clusterSelectionFactory;
  }

  private YTDatabaseSessionInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  public IntSet getBlobClusters() {
    return blogClusters;
  }

  @Override
  public YTView getView(String name) {
    if (name == null) {
      return null;
    }

    YTView cls = views.get(name.toLowerCase(Locale.ENGLISH));
    return cls;
  }

  @Override
  public YTView createView(String viewName, String statement) {
    throw new UnsupportedOperationException();
  }

  public YTView createView(
      YTDatabaseSessionInternal database,
      final String viewName,
      String statement,
      Map<String, Object> metadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTView createView(OViewConfig config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTView createView(OViewConfig config, ViewCreationListener listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsView(String name) {
    return views.containsKey(name.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public void dropView(String name) {
    throw new UnsupportedOperationException();
  }
}
