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
package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ProxedResource;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class IndexManagerProxy extends ProxedResource<IndexManagerAbstract>
    implements IndexManager {

  public IndexManagerProxy(
      final IndexManagerAbstract iDelegate, final DatabaseSessionInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  public void load() {
  }

  /**
   * Force reloading of indexes.
   */
  public IndexManagerProxy reload() {
    delegate.load(database);
    return this;
  }

  public void create() {
    delegate.create(database);
  }

  public Collection<? extends Index> getIndexes() {
    return delegate.getIndexes(database);
  }

  public Index getIndex(final String iName) {
    return delegate.getIndex(database, iName);
  }

  public boolean existsIndex(final String iName) {
    return delegate.existsIndex(iName);
  }

  public Index createIndex(
      final String iName,
      final String iType,
      final IndexDefinition indexDefinition,
      final int[] clusterIdsToIndex,
      final ProgressListener progressListener,
      final Map<String, ?> metadata) {
    return delegate.createIndex(
        database, iName, iType, indexDefinition, clusterIdsToIndex, progressListener, metadata);
  }

  @Override
  public Index createIndex(
      final String iName,
      final String iType,
      final IndexDefinition iIndexDefinition,
      final int[] iClusterIdsToIndex,
      final ProgressListener progressListener,
      final Map<String, ?> metadata,
      final String algorithm) {
    return delegate.createIndex(
        database,
        iName,
        iType,
        iIndexDefinition,
        iClusterIdsToIndex,
        progressListener,
        metadata,
        algorithm);
  }

  public EntityImpl getConfiguration(DatabaseSession session) {
    return delegate.getConfiguration((DatabaseSessionInternal) session);
  }

  public IndexManager dropIndex(final String iIndexName) {
    delegate.dropIndex(database, iIndexName);
    return this;
  }

  public String getDefaultClusterName() {
    return delegate.getDefaultClusterName();
  }

  public void setDefaultClusterName(final String defaultClusterName) {
    delegate.setDefaultClusterName(database, defaultClusterName);
  }

  public Set<Index> getClassInvolvedIndexes(
      final String className, final Collection<String> fields) {
    return delegate.getClassInvolvedIndexes(database, className, fields);
  }

  public Set<Index> getClassInvolvedIndexes(final String className, final String... fields) {
    return delegate.getClassInvolvedIndexes(database, className, fields);
  }

  public boolean areIndexed(final String className, final Collection<String> fields) {
    return delegate.areIndexed(className, fields);
  }

  public boolean areIndexed(final String className, final String... fields) {
    return delegate.areIndexed(className, fields);
  }

  public Set<Index> getClassIndexes(final String className) {
    return delegate.getClassIndexes(database, className);
  }

  @Override
  public void getClassIndexes(final String className, final Collection<Index> indexes) {
    delegate.getClassIndexes(database, className, indexes);
  }

  public Index getClassIndex(final String className, final String indexName) {
    return delegate.getClassIndex(database, className, indexName);
  }

  @Override
  public IndexUnique getClassUniqueIndex(final String className) {
    return delegate.getClassUniqueIndex(className);
  }

  @Override
  public void recreateIndexes() {
    delegate.recreateIndexes(database);
  }

  @Override
  public void waitTillIndexRestore() {
    delegate.waitTillIndexRestore();
  }

  @Override
  public boolean autoRecreateIndexesAfterCrash() {
    return delegate.autoRecreateIndexesAfterCrash(database);
  }

  @Override
  public void addClusterToIndex(DatabaseSession session, String clusterName, String indexName) {
    delegate.addClusterToIndex((DatabaseSessionInternal) session, clusterName, indexName);
  }

  @Override
  public void removeClusterFromIndex(DatabaseSession session, String clusterName,
      String indexName) {
    delegate.removeClusterFromIndex((DatabaseSessionInternal) session, clusterName, indexName);
  }

  @Override
  public IndexManager save(DatabaseSession session) {
    delegate.save((DatabaseSessionInternal) session);
    return this;
  }

  public void removeClassPropertyIndex(DatabaseSession session, final Index idx) {
    //noinspection deprecation
    delegate.removeClassPropertyIndex((DatabaseSessionInternal) session, idx);
  }

  public void getClassRawIndexes(String name, Collection<Index> indexes) {
    delegate.getClassRawIndexes(name, indexes);
  }

  public IndexManagerAbstract delegate() {
    return delegate;
  }
}
