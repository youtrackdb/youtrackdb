/*
 * Copyright YouTrackDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.clusterselection.ClusterSelectionStrategy;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract Delegate for SchemaClass interface.
 */
public abstract class SchemaClassAbstractDelegate implements SchemaClass {

  protected final SchemaClass delegate;

  public SchemaClassAbstractDelegate(final SchemaClass delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException("Class is null");
    }

    this.delegate = delegate;
  }

  @Override
  public Index getAutoShardingIndex(DatabaseSession session) {
    return delegate.getAutoShardingIndex(session);
  }

  @Override
  public boolean isStrictMode() {
    return delegate.isStrictMode();
  }

  @Override
  public boolean isAbstract() {
    return delegate.isAbstract();
  }

  @Override
  public SchemaClass setAbstract(DatabaseSession session, final boolean iAbstract) {
    delegate.setAbstract(session, iAbstract);
    return this;
  }

  @Override
  public SchemaClass setStrictMode(DatabaseSession session, final boolean iMode) {
    delegate.setStrictMode(session, iMode);
    return this;
  }

  @Override
  @Deprecated
  public SchemaClass getSuperClass() {
    return delegate.getSuperClass();
  }

  @Override
  @Deprecated
  public SchemaClass setSuperClass(DatabaseSession session, final SchemaClass iSuperClass) {
    delegate.setSuperClass(session, iSuperClass);
    return this;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public List<SchemaClass> getSuperClasses() {
    return delegate.getSuperClasses();
  }

  @Override
  public boolean hasSuperClasses() {
    return delegate.hasSuperClasses();
  }

  @Override
  public SchemaClass setSuperClasses(DatabaseSession session,
      final List<? extends SchemaClass> classes) {
    delegate.setSuperClasses(session, classes);
    return this;
  }

  @Override
  public List<String> getSuperClassesNames() {
    return delegate.getSuperClassesNames();
  }

  @Override
  public void getIndexes(DatabaseSession session, final Collection<Index> indexes) {
    delegate.getIndexes(session, indexes);
  }

  @Override
  public SchemaClass addSuperClass(DatabaseSession session, final SchemaClass superClass) {
    delegate.addSuperClass(session, superClass);
    return this;
  }

  @Override
  public SchemaClass removeSuperClass(DatabaseSession session, final SchemaClass superClass) {
    delegate.removeSuperClass(session, superClass);
    return this;
  }

  @Override
  public SchemaClass setName(DatabaseSession session, final String iName) {
    delegate.setName(session, iName);
    return this;
  }

  @Override
  public String getStreamableName() {
    return delegate.getStreamableName();
  }

  @Override
  public Collection<Property> declaredProperties() {
    return delegate.declaredProperties();
  }

  @Override
  public Collection<Property> properties(DatabaseSession session) {
    return delegate.properties(session);
  }

  @Override
  public Map<String, Property> propertiesMap(DatabaseSession session) {
    return delegate.propertiesMap(session);
  }

  @Override
  public Collection<Property> getIndexedProperties(DatabaseSession session) {
    return delegate.getIndexedProperties(session);
  }

  @Override
  public Property getProperty(String iPropertyName) {
    return delegate.getProperty(iPropertyName);
  }

  @Override
  public Property createProperty(DatabaseSession session, final String iPropertyName,
      final PropertyType iType) {
    return delegate.createProperty(session, iPropertyName, iType);
  }

  @Override
  public Property createProperty(
      DatabaseSession session, final String iPropertyName, final PropertyType iType,
      final SchemaClass iLinkedClass) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedClass);
  }

  @Override
  public Property createProperty(
      DatabaseSession session, String iPropertyName, PropertyType iType, SchemaClass iLinkedClass,
      boolean unsafe) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedClass, unsafe);
  }

  @Override
  public Property createProperty(
      DatabaseSession session, final String iPropertyName, final PropertyType iType,
      final PropertyType iLinkedType) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedType);
  }

  @Override
  public Property createProperty(
      DatabaseSession session, String iPropertyName, PropertyType iType, PropertyType iLinkedType,
      boolean unsafe) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedType, unsafe);
  }

  @Override
  public void dropProperty(DatabaseSession session, final String iPropertyName) {
    delegate.dropProperty(session, iPropertyName);
  }

  @Override
  public boolean existsProperty(final String iPropertyName) {
    return delegate.existsProperty(iPropertyName);
  }

  @Override
  public int getClusterForNewInstance(final EntityImpl doc) {
    return delegate.getClusterForNewInstance(doc);
  }

  @Override
  public int getDefaultClusterId() {
    return delegate.getDefaultClusterId();
  }

  @Override
  public void setDefaultClusterId(DatabaseSession session, final int iDefaultClusterId) {
    delegate.setDefaultClusterId(session, iDefaultClusterId);
  }

  @Override
  public int[] getClusterIds() {
    return delegate.getClusterIds();
  }

  @Override
  public SchemaClass addClusterId(DatabaseSession session, final int iId) {
    delegate.addClusterId(session, iId);
    return this;
  }

  @Override
  public ClusterSelectionStrategy getClusterSelection() {
    return delegate.getClusterSelection();
  }

  @Override
  public SchemaClass setClusterSelection(DatabaseSession session,
      final ClusterSelectionStrategy clusterSelection) {
    delegate.setClusterSelection(session, clusterSelection);
    return this;
  }

  @Override
  public SchemaClass setClusterSelection(DatabaseSession session, final String iStrategyName) {
    delegate.setClusterSelection(session, iStrategyName);
    return this;
  }

  @Override
  public SchemaClass addCluster(DatabaseSession session, final String iClusterName) {
    delegate.addCluster(session, iClusterName);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchemaClass truncateCluster(DatabaseSession session, String clusterName) {
    delegate.truncateCluster(session, clusterName);

    return this;
  }

  @Override
  public SchemaClass removeClusterId(DatabaseSession session, final int iId) {
    delegate.removeClusterId(session, iId);
    return this;
  }

  @Override
  public int[] getPolymorphicClusterIds() {
    return delegate.getPolymorphicClusterIds();
  }

  @Override
  public Collection<SchemaClass> getSubclasses() {
    return delegate.getSubclasses();
  }

  @Override
  @Deprecated
  public Collection<SchemaClass> getBaseClasses() {
    return delegate.getSubclasses();
  }

  @Override
  public Collection<SchemaClass> getAllSubclasses() {
    return delegate.getAllSubclasses();
  }

  @Override
  public Collection<SchemaClass> getAllSuperClasses() {
    return delegate.getAllSuperClasses();
  }

  @Override
  @Deprecated
  public Collection<SchemaClass> getAllBaseClasses() {
    return delegate.getAllSubclasses();
  }

  @Override
  public long getSize(DatabaseSessionInternal session) {
    return delegate.getSize(session);
  }

  @Override
  public float getOverSize() {
    return delegate.getOverSize();
  }

  @Override
  public SchemaClass setOverSize(DatabaseSession session, final float overSize) {
    delegate.setOverSize(session, overSize);
    return this;
  }

  @Override
  public long count(DatabaseSession session) {
    return delegate.count(session);
  }

  @Override
  public long count(DatabaseSession session, final boolean iPolymorphic) {
    return delegate.count(session, iPolymorphic);
  }

  @Override
  public void truncate(DatabaseSession session) throws IOException {
    delegate.truncate(session);
  }

  @Override
  public boolean isSubClassOf(final String iClassName) {
    return delegate.isSubClassOf(iClassName);
  }

  @Override
  public boolean isSubClassOf(final SchemaClass iClass) {
    return delegate.isSubClassOf(iClass);
  }

  @Override
  public boolean isSuperClassOf(final SchemaClass iClass) {
    return delegate.isSuperClassOf(iClass);
  }

  @Override
  public String getShortName() {
    return delegate.getShortName();
  }

  @Override
  public SchemaClass setShortName(DatabaseSession session, final String shortName) {
    delegate.setShortName(session, shortName);
    return this;
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public SchemaClass setDescription(DatabaseSession session, String iDescription) {
    delegate.setDescription(session, iDescription);
    return this;
  }

  @Override
  public Object get(ATTRIBUTES iAttribute) {
    return delegate.get(iAttribute);
  }

  @Override
  public SchemaClass set(DatabaseSession session, ATTRIBUTES attribute, Object iValue) {
    delegate.set(session, attribute, iValue);
    return this;
  }

  @Override
  public Index createIndex(DatabaseSession session, final String iName, final INDEX_TYPE iType,
      final String... fields) {
    return delegate.createIndex(session, iName, iType, fields);
  }

  @Override
  public Index createIndex(DatabaseSession session, final String iName, final String iType,
      final String... fields) {
    return delegate.createIndex(session, iName, iType, fields);
  }

  @Override
  public Index createIndex(
      DatabaseSession session, final String iName,
      final INDEX_TYPE iType,
      final ProgressListener iProgressListener,
      final String... fields) {
    return delegate.createIndex(session, iName, iType, iProgressListener, fields);
  }

  @Override
  public Index createIndex(
      DatabaseSession session, final String iName,
      final String iType,
      final ProgressListener iProgressListener,
      final EntityImpl metadata,
      String algorithm,
      String... fields) {
    return delegate.createIndex(session, iName, iType, iProgressListener, metadata, algorithm,
        fields);
  }

  @Override
  public Index createIndex(
      DatabaseSession session, final String iName,
      final String iType,
      final ProgressListener iProgressListener,
      final EntityImpl metadata,
      String... fields) {
    return delegate.createIndex(session, iName, iType, iProgressListener, metadata, fields);
  }

  @Override
  public Set<Index> getInvolvedIndexes(DatabaseSession session,
      final Collection<String> fields) {
    return delegate.getInvolvedIndexes(session, fields);
  }

  @Override
  public Set<Index> getInvolvedIndexes(DatabaseSession session, final String... fields) {
    return delegate.getInvolvedIndexes(session, fields);
  }

  @Override
  public Set<Index> getClassInvolvedIndexes(DatabaseSession session,
      final Collection<String> fields) {
    return delegate.getClassInvolvedIndexes(session, fields);
  }

  @Override
  public Set<Index> getClassInvolvedIndexes(DatabaseSession session, final String... fields) {
    return delegate.getClassInvolvedIndexes(session, fields);
  }

  @Override
  public boolean areIndexed(DatabaseSession session, final Collection<String> fields) {
    return delegate.areIndexed(session, fields);
  }

  @Override
  public boolean areIndexed(DatabaseSession session, final String... fields) {
    return delegate.areIndexed(session, fields);
  }

  @Override
  public Index getClassIndex(DatabaseSession session, final String iName) {
    return delegate.getClassIndex(session, iName);
  }

  @Override
  public Set<Index> getClassIndexes(DatabaseSession session) {
    return delegate.getClassIndexes(session);
  }

  @Override
  public void getClassIndexes(DatabaseSession session, final Collection<Index> indexes) {
    delegate.getClassIndexes(session, indexes);
  }

  @Override
  public Set<Index> getIndexes(DatabaseSession session) {
    return delegate.getIndexes(session);
  }

  @Override
  public String getCustom(final String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public SchemaClass setCustom(DatabaseSession session, final String iName, String iValue) {
    delegate.setCustom(session, iName, iValue);
    return this;
  }

  @Override
  public void removeCustom(DatabaseSession session, final String iName) {
    delegate.removeCustom(session, iName);
  }

  @Override
  public void clearCustom(DatabaseSession session) {
    delegate.clearCustom(session);
  }

  @Override
  public Set<String> getCustomKeys() {
    return delegate.getCustomKeys();
  }

  @Override
  public boolean hasClusterId(final int clusterId) {
    return delegate.hasClusterId(clusterId);
  }

  @Override
  public boolean hasPolymorphicClusterId(final int clusterId) {
    return delegate.hasPolymorphicClusterId(clusterId);
  }

  @Override
  public int compareTo(final SchemaClass o) {
    return delegate.compareTo(o);
  }

  @Override
  public float getClassOverSize() {
    return delegate.getClassOverSize();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public boolean equals(final Object obj) {
    return delegate.equals(obj);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  public SchemaClass getDelegate() {
    return delegate;
  }
}
