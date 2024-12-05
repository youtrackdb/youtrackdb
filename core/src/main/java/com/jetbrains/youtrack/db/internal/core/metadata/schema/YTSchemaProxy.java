/*
 *
 *  *  Copyright YouTrackDB
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

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.OProxedResource;
import com.jetbrains.youtrack.db.internal.core.db.viewmanager.ViewCreationListener;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Proxy class to use the shared OSchemaShared instance. Before to delegate each operations it sets
 * the current database in the thread local.
 */
@SuppressWarnings("unchecked")
public class YTSchemaProxy extends OProxedResource<OSchemaShared> implements YTSchema {

  public YTSchemaProxy(final OSchemaShared iDelegate, final YTDatabaseSessionInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  @Override
  public YTImmutableSchema makeSnapshot() {
    return delegate.makeSnapshot(database);
  }

  public void create() {
    delegate.create(database);
  }

  public int countClasses() {
    return delegate.countClasses(database);
  }

  public int countViews() {
    return delegate.countViews(database);
  }

  public YTClass createClass(final String iClassName) {
    return delegate.createClass(database, iClassName);
  }

  public YTClass getOrCreateClass(final String iClassName) {
    return getOrCreateClass(iClassName, (YTClass) null);
  }

  public YTClass getOrCreateClass(final String iClassName, final YTClass iSuperClass) {
    if (iClassName == null) {
      return null;
    }

    YTClass cls = delegate.getClass(iClassName.toLowerCase(Locale.ENGLISH));
    if (cls != null) {
      return cls;
    }

    cls = delegate.getOrCreateClass(database, iClassName, iSuperClass);

    return cls;
  }

  @Override
  public YTClass getOrCreateClass(String iClassName, YTClass... superClasses) {
    return delegate.getOrCreateClass(database, iClassName, superClasses);
  }

  @Override
  public YTClass createClass(final String iClassName, final YTClass iSuperClass) {
    return delegate.createClass(database, iClassName, iSuperClass, null);
  }

  @Override
  public YTClass createClass(String iClassName, YTClass... superClasses) {
    return delegate.createClass(database, iClassName, superClasses);
  }

  public YTClass createClass(
      final String iClassName, final YTClass iSuperClass, final int[] iClusterIds) {
    return delegate.createClass(database, iClassName, iSuperClass, iClusterIds);
  }

  @Override
  public YTClass createClass(String className, int[] clusterIds, YTClass... superClasses) {
    return delegate.createClass(database, className, clusterIds, superClasses);
  }

  @Override
  public YTClass createAbstractClass(final String iClassName) {
    return delegate.createAbstractClass(database, iClassName);
  }

  @Override
  public YTClass createAbstractClass(final String iClassName, final YTClass iSuperClass) {
    return delegate.createAbstractClass(database, iClassName, iSuperClass);
  }

  @Override
  public YTClass createAbstractClass(String iClassName, YTClass... superClasses) {
    return delegate.createAbstractClass(database, iClassName, superClasses);
  }

  public void dropClass(final String iClassName) {
    delegate.dropClass(database, iClassName);
  }

  public boolean existsClass(final String iClassName) {
    if (iClassName == null) {
      return false;
    }

    return delegate.existsClass(iClassName.toLowerCase(Locale.ENGLISH));
  }

  public boolean existsView(final String name) {
    if (name == null) {
      return false;
    }

    return delegate.existsView(name.toLowerCase(Locale.ENGLISH));
  }

  public void dropView(final String name) {
    delegate.dropView(database, name);
  }

  public YTClass getClass(final Class<?> iClass) {
    if (iClass == null) {
      return null;
    }

    return delegate.getClass(iClass);
  }

  public YTClass getClass(final String iClassName) {
    if (iClassName == null) {
      return null;
    }

    return delegate.getClass(iClassName);
  }

  public Collection<YTClass> getClasses() {
    return delegate.getClasses(database);
  }

  public Collection<YTView> getViews() {
    return delegate.getViews(database);
  }

  @Deprecated
  public void load() {

    delegate.load(database);
  }

  public YTView getView(final String name) {
    if (name == null) {
      return null;
    }

    return delegate.getView(name);
  }

  @Override
  public YTView createView(String viewName, String statement) {
    return createView(database, viewName, statement, new HashMap<>());
  }

  public YTView createView(
      YTDatabaseSessionInternal database,
      final String viewName,
      String statement,
      Map<String, Object> metadata) {
    return delegate.createView(database, viewName, statement, metadata);
  }

  @Override
  public YTView createView(OViewConfig config) {
    return delegate.createView(database, config);
  }

  public YTView createView(OViewConfig config, ViewCreationListener listener) {
    return delegate.createView(database, config, listener);
  }

  public YTSchema reload() {
    delegate.reload(database);
    return this;
  }

  public int getVersion() {

    return delegate.getVersion();
  }

  public YTRID getIdentity() {

    return delegate.getIdentity();
  }

  @Deprecated
  public void close() {
    // DO NOTHING THE DELEGATE CLOSE IS MANAGED IN A DIFFERENT CONTEXT
  }

  public String toString() {

    return delegate.toString();
  }

  @Override
  public Set<YTClass> getClassesRelyOnCluster(final String iClusterName) {
    return delegate.getClassesRelyOnCluster(database, iClusterName);
  }

  @Override
  public YTClass createClass(String className, int clusters, YTClass... superClasses) {
    return delegate.createClass(database, className, clusters, superClasses);
  }

  @Override
  public YTClass getClassByClusterId(int clusterId) {
    return delegate.getClassByClusterId(clusterId);
  }

  @Override
  public YTView getViewByClusterId(int clusterId) {
    return delegate.getViewByClusterId(clusterId);
  }

  @Override
  public OGlobalProperty getGlobalPropertyById(int id) {
    return delegate.getGlobalPropertyById(id);
  }

  @Override
  public List<OGlobalProperty> getGlobalProperties() {
    return delegate.getGlobalProperties();
  }

  public OGlobalProperty createGlobalProperty(String name, YTType type, Integer id) {
    return delegate.createGlobalProperty(name, type, id);
  }

  @Override
  public OClusterSelectionFactory getClusterSelectionFactory() {
    return delegate.getClusterSelectionFactory();
  }

  public IntSet getBlobClusters() {
    return delegate.getBlobClusters();
  }

  public int addBlobCluster(final int clusterId) {
    return delegate.addBlobCluster(database, clusterId);
  }

  public void removeBlobCluster(String clusterName) {
    delegate.removeBlobCluster(database, clusterName);
  }
}
