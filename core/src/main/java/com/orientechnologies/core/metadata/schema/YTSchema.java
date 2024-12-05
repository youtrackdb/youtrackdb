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
package com.orientechnologies.core.metadata.schema;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.schema.clusterselection.OClusterSelectionFactory;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface YTSchema {

  int countClasses();

  int countViews();

  YTClass createClass(String iClassName);

  YTClass createClass(String iClassName, YTClass iSuperClass);

  YTClass createClass(String className, int clusters, YTClass... superClasses);

  YTClass createClass(String iClassName, YTClass... superClasses);

  YTClass createClass(String iClassName, YTClass iSuperClass, int[] iClusterIds);

  YTClass createClass(String className, int[] clusterIds, YTClass... superClasses);

  YTClass createAbstractClass(String iClassName);

  YTClass createAbstractClass(String iClassName, YTClass iSuperClass);

  YTClass createAbstractClass(String iClassName, YTClass... superClasses);

  void dropClass(String iClassName);

  YTSchema reload();

  boolean existsClass(String iClassName);

  YTClass getClass(Class<?> iClass);

  /**
   * Returns the YTClass instance by class name.
   *
   * <p>If the class is not configured and the database has an entity manager with the requested
   * class as registered, then creates a schema class for it at the fly.
   *
   * <p>If the database nor the entity manager have not registered class with specified name,
   * returns null.
   *
   * @param iClassName Name of the class to retrieve
   * @return class instance or null if class with given name is not configured.
   */
  YTClass getClass(String iClassName);

  YTClass getOrCreateClass(String iClassName);

  YTClass getOrCreateClass(String iClassName, YTClass iSuperClass);

  YTClass getOrCreateClass(String iClassName, YTClass... superClasses);

  Collection<YTClass> getClasses();

  Collection<YTView> getViews();

  YTView getView(String name);

  YTView createView(final String viewName, String statement);

  YTView createView(
      YTDatabaseSessionInternal database,
      final String viewName,
      String statement,
      Map<String, Object> metadata);

  YTView createView(OViewConfig config);

  YTView createView(OViewConfig config, ViewCreationListener listener);

  boolean existsView(String name);

  void dropView(String name);

  @Deprecated
  void create();

  @Deprecated
  int getVersion();

  YTRID getIdentity();

  /**
   * Returns all the classes that rely on a cluster
   *
   * @param iClusterName Cluster name
   */
  Set<YTClass> getClassesRelyOnCluster(String iClusterName);

  YTClass getClassByClusterId(int clusterId);

  YTView getViewByClusterId(int clusterId);

  OGlobalProperty getGlobalPropertyById(int id);

  List<OGlobalProperty> getGlobalProperties();

  OGlobalProperty createGlobalProperty(String name, YTType type, Integer id);

  OClusterSelectionFactory getClusterSelectionFactory();

  YTImmutableSchema makeSnapshot();
}
