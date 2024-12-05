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

package com.orientechnologies.core.metadata.schema;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract Delegate for YTClass interface.
 */
public abstract class YTClassAbstractDelegate implements YTClass {

  protected final YTClass delegate;

  public YTClassAbstractDelegate(final YTClass delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException("Class is null");
    }

    this.delegate = delegate;
  }

  @Override
  public OIndex getAutoShardingIndex(YTDatabaseSession session) {
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
  public YTClass setAbstract(YTDatabaseSession session, final boolean iAbstract) {
    delegate.setAbstract(session, iAbstract);
    return this;
  }

  @Override
  public YTClass setStrictMode(YTDatabaseSession session, final boolean iMode) {
    delegate.setStrictMode(session, iMode);
    return this;
  }

  @Override
  @Deprecated
  public YTClass getSuperClass() {
    return delegate.getSuperClass();
  }

  @Override
  @Deprecated
  public YTClass setSuperClass(YTDatabaseSession session, final YTClass iSuperClass) {
    delegate.setSuperClass(session, iSuperClass);
    return this;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public List<YTClass> getSuperClasses() {
    return delegate.getSuperClasses();
  }

  @Override
  public boolean hasSuperClasses() {
    return delegate.hasSuperClasses();
  }

  @Override
  public YTClass setSuperClasses(YTDatabaseSession session, final List<? extends YTClass> classes) {
    delegate.setSuperClasses(session, classes);
    return this;
  }

  @Override
  public List<String> getSuperClassesNames() {
    return delegate.getSuperClassesNames();
  }

  @Override
  public void getIndexes(YTDatabaseSession session, final Collection<OIndex> indexes) {
    delegate.getIndexes(session, indexes);
  }

  @Override
  public YTClass addSuperClass(YTDatabaseSession session, final YTClass superClass) {
    delegate.addSuperClass(session, superClass);
    return this;
  }

  @Override
  public YTClass removeSuperClass(YTDatabaseSession session, final YTClass superClass) {
    delegate.removeSuperClass(session, superClass);
    return this;
  }

  @Override
  public YTClass setName(YTDatabaseSession session, final String iName) {
    delegate.setName(session, iName);
    return this;
  }

  @Override
  public String getStreamableName() {
    return delegate.getStreamableName();
  }

  @Override
  public Collection<YTProperty> declaredProperties() {
    return delegate.declaredProperties();
  }

  @Override
  public Collection<YTProperty> properties(YTDatabaseSession session) {
    return delegate.properties(session);
  }

  @Override
  public Map<String, YTProperty> propertiesMap(YTDatabaseSession session) {
    return delegate.propertiesMap(session);
  }

  @Override
  public Collection<YTProperty> getIndexedProperties(YTDatabaseSession session) {
    return delegate.getIndexedProperties(session);
  }

  @Override
  public YTProperty getProperty(String iPropertyName) {
    return delegate.getProperty(iPropertyName);
  }

  @Override
  public YTProperty createProperty(YTDatabaseSession session, final String iPropertyName,
      final YTType iType) {
    return delegate.createProperty(session, iPropertyName, iType);
  }

  @Override
  public YTProperty createProperty(
      YTDatabaseSession session, final String iPropertyName, final YTType iType,
      final YTClass iLinkedClass) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedClass);
  }

  @Override
  public YTProperty createProperty(
      YTDatabaseSession session, String iPropertyName, YTType iType, YTClass iLinkedClass,
      boolean unsafe) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedClass, unsafe);
  }

  @Override
  public YTProperty createProperty(
      YTDatabaseSession session, final String iPropertyName, final YTType iType,
      final YTType iLinkedType) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedType);
  }

  @Override
  public YTProperty createProperty(
      YTDatabaseSession session, String iPropertyName, YTType iType, YTType iLinkedType,
      boolean unsafe) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedType, unsafe);
  }

  @Override
  public void dropProperty(YTDatabaseSession session, final String iPropertyName) {
    delegate.dropProperty(session, iPropertyName);
  }

  @Override
  public boolean existsProperty(final String iPropertyName) {
    return delegate.existsProperty(iPropertyName);
  }

  @Override
  public int getClusterForNewInstance(final YTEntityImpl doc) {
    return delegate.getClusterForNewInstance(doc);
  }

  @Override
  public int getDefaultClusterId() {
    return delegate.getDefaultClusterId();
  }

  @Override
  public void setDefaultClusterId(YTDatabaseSession session, final int iDefaultClusterId) {
    delegate.setDefaultClusterId(session, iDefaultClusterId);
  }

  @Override
  public int[] getClusterIds() {
    return delegate.getClusterIds();
  }

  @Override
  public YTClass addClusterId(YTDatabaseSession session, final int iId) {
    delegate.addClusterId(session, iId);
    return this;
  }

  @Override
  public OClusterSelectionStrategy getClusterSelection() {
    return delegate.getClusterSelection();
  }

  @Override
  public YTClass setClusterSelection(YTDatabaseSession session,
      final OClusterSelectionStrategy clusterSelection) {
    delegate.setClusterSelection(session, clusterSelection);
    return this;
  }

  @Override
  public YTClass setClusterSelection(YTDatabaseSession session, final String iStrategyName) {
    delegate.setClusterSelection(session, iStrategyName);
    return this;
  }

  @Override
  public YTClass addCluster(YTDatabaseSession session, final String iClusterName) {
    delegate.addCluster(session, iClusterName);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public YTClass truncateCluster(YTDatabaseSession session, String clusterName) {
    delegate.truncateCluster(session, clusterName);

    return this;
  }

  @Override
  public YTClass removeClusterId(YTDatabaseSession session, final int iId) {
    delegate.removeClusterId(session, iId);
    return this;
  }

  @Override
  public int[] getPolymorphicClusterIds() {
    return delegate.getPolymorphicClusterIds();
  }

  @Override
  public Collection<YTClass> getSubclasses() {
    return delegate.getSubclasses();
  }

  @Override
  @Deprecated
  public Collection<YTClass> getBaseClasses() {
    return delegate.getSubclasses();
  }

  @Override
  public Collection<YTClass> getAllSubclasses() {
    return delegate.getAllSubclasses();
  }

  @Override
  public Collection<YTClass> getAllSuperClasses() {
    return delegate.getAllSuperClasses();
  }

  @Override
  @Deprecated
  public Collection<YTClass> getAllBaseClasses() {
    return delegate.getAllSubclasses();
  }

  @Override
  public long getSize(YTDatabaseSessionInternal session) {
    return delegate.getSize(session);
  }

  @Override
  public float getOverSize() {
    return delegate.getOverSize();
  }

  @Override
  public YTClass setOverSize(YTDatabaseSession session, final float overSize) {
    delegate.setOverSize(session, overSize);
    return this;
  }

  @Override
  public long count(YTDatabaseSession session) {
    return delegate.count(session);
  }

  @Override
  public long count(YTDatabaseSession session, final boolean iPolymorphic) {
    return delegate.count(session, iPolymorphic);
  }

  @Override
  public void truncate(YTDatabaseSession session) throws IOException {
    delegate.truncate(session);
  }

  @Override
  public boolean isSubClassOf(final String iClassName) {
    return delegate.isSubClassOf(iClassName);
  }

  @Override
  public boolean isSubClassOf(final YTClass iClass) {
    return delegate.isSubClassOf(iClass);
  }

  @Override
  public boolean isSuperClassOf(final YTClass iClass) {
    return delegate.isSuperClassOf(iClass);
  }

  @Override
  public String getShortName() {
    return delegate.getShortName();
  }

  @Override
  public YTClass setShortName(YTDatabaseSession session, final String shortName) {
    delegate.setShortName(session, shortName);
    return this;
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public YTClass setDescription(YTDatabaseSession session, String iDescription) {
    delegate.setDescription(session, iDescription);
    return this;
  }

  @Override
  public Object get(ATTRIBUTES iAttribute) {
    return delegate.get(iAttribute);
  }

  @Override
  public YTClass set(YTDatabaseSession session, ATTRIBUTES attribute, Object iValue) {
    delegate.set(session, attribute, iValue);
    return this;
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, final String iName, final INDEX_TYPE iType,
      final String... fields) {
    return delegate.createIndex(session, iName, iType, fields);
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, final String iName, final String iType,
      final String... fields) {
    return delegate.createIndex(session, iName, iType, fields);
  }

  @Override
  public OIndex createIndex(
      YTDatabaseSession session, final String iName,
      final INDEX_TYPE iType,
      final OProgressListener iProgressListener,
      final String... fields) {
    return delegate.createIndex(session, iName, iType, iProgressListener, fields);
  }

  @Override
  public OIndex createIndex(
      YTDatabaseSession session, final String iName,
      final String iType,
      final OProgressListener iProgressListener,
      final YTEntityImpl metadata,
      String algorithm,
      String... fields) {
    return delegate.createIndex(session, iName, iType, iProgressListener, metadata, algorithm,
        fields);
  }

  @Override
  public OIndex createIndex(
      YTDatabaseSession session, final String iName,
      final String iType,
      final OProgressListener iProgressListener,
      final YTEntityImpl metadata,
      String... fields) {
    return delegate.createIndex(session, iName, iType, iProgressListener, metadata, fields);
  }

  @Override
  public Set<OIndex> getInvolvedIndexes(YTDatabaseSession session,
      final Collection<String> fields) {
    return delegate.getInvolvedIndexes(session, fields);
  }

  @Override
  public Set<OIndex> getInvolvedIndexes(YTDatabaseSession session, final String... fields) {
    return delegate.getInvolvedIndexes(session, fields);
  }

  @Override
  public Set<OIndex> getClassInvolvedIndexes(YTDatabaseSession session,
      final Collection<String> fields) {
    return delegate.getClassInvolvedIndexes(session, fields);
  }

  @Override
  public Set<OIndex> getClassInvolvedIndexes(YTDatabaseSession session, final String... fields) {
    return delegate.getClassInvolvedIndexes(session, fields);
  }

  @Override
  public boolean areIndexed(YTDatabaseSession session, final Collection<String> fields) {
    return delegate.areIndexed(session, fields);
  }

  @Override
  public boolean areIndexed(YTDatabaseSession session, final String... fields) {
    return delegate.areIndexed(session, fields);
  }

  @Override
  public OIndex getClassIndex(YTDatabaseSession session, final String iName) {
    return delegate.getClassIndex(session, iName);
  }

  @Override
  public Set<OIndex> getClassIndexes(YTDatabaseSession session) {
    return delegate.getClassIndexes(session);
  }

  @Override
  public void getClassIndexes(YTDatabaseSession session, final Collection<OIndex> indexes) {
    delegate.getClassIndexes(session, indexes);
  }

  @Override
  public Set<OIndex> getIndexes(YTDatabaseSession session) {
    return delegate.getIndexes(session);
  }

  @Override
  public String getCustom(final String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public YTClass setCustom(YTDatabaseSession session, final String iName, String iValue) {
    delegate.setCustom(session, iName, iValue);
    return this;
  }

  @Override
  public void removeCustom(YTDatabaseSession session, final String iName) {
    delegate.removeCustom(session, iName);
  }

  @Override
  public void clearCustom(YTDatabaseSession session) {
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
  public int compareTo(final YTClass o) {
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

  public YTClass getDelegate() {
    return delegate;
  }
}
