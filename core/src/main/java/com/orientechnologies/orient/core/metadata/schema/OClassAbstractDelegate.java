/*
 * Copyright OxygenDB
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

package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract Delegate for OClass interface.
 */
public abstract class OClassAbstractDelegate implements OClass {

  protected final OClass delegate;

  public OClassAbstractDelegate(final OClass delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException("Class is null");
    }

    this.delegate = delegate;
  }

  @Override
  public OIndex getAutoShardingIndex(ODatabaseSession session) {
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
  public OClass setAbstract(ODatabaseSession session, final boolean iAbstract) {
    delegate.setAbstract(session, iAbstract);
    return this;
  }

  @Override
  public OClass setStrictMode(ODatabaseSession session, final boolean iMode) {
    delegate.setStrictMode(session, iMode);
    return this;
  }

  @Override
  @Deprecated
  public OClass getSuperClass() {
    return delegate.getSuperClass();
  }

  @Override
  @Deprecated
  public OClass setSuperClass(ODatabaseSession session, final OClass iSuperClass) {
    delegate.setSuperClass(session, iSuperClass);
    return this;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public List<OClass> getSuperClasses() {
    return delegate.getSuperClasses();
  }

  @Override
  public boolean hasSuperClasses() {
    return delegate.hasSuperClasses();
  }

  @Override
  public OClass setSuperClasses(ODatabaseSession session, final List<? extends OClass> classes) {
    delegate.setSuperClasses(session, classes);
    return this;
  }

  @Override
  public List<String> getSuperClassesNames() {
    return delegate.getSuperClassesNames();
  }

  @Override
  public void getIndexes(ODatabaseSession session, final Collection<OIndex> indexes) {
    delegate.getIndexes(session, indexes);
  }

  @Override
  public OClass addSuperClass(ODatabaseSession session, final OClass superClass) {
    delegate.addSuperClass(session, superClass);
    return this;
  }

  @Override
  public OClass removeSuperClass(ODatabaseSession session, final OClass superClass) {
    delegate.removeSuperClass(session, superClass);
    return this;
  }

  @Override
  public OClass setName(ODatabaseSession session, final String iName) {
    delegate.setName(session, iName);
    return this;
  }

  @Override
  public String getStreamableName() {
    return delegate.getStreamableName();
  }

  @Override
  public Collection<OProperty> declaredProperties() {
    return delegate.declaredProperties();
  }

  @Override
  public Collection<OProperty> properties(ODatabaseSession session) {
    return delegate.properties(session);
  }

  @Override
  public Map<String, OProperty> propertiesMap(ODatabaseSession session) {
    return delegate.propertiesMap(session);
  }

  @Override
  public Collection<OProperty> getIndexedProperties(ODatabaseSession session) {
    return delegate.getIndexedProperties(session);
  }

  @Override
  public OProperty getProperty(String iPropertyName) {
    return delegate.getProperty(iPropertyName);
  }

  @Override
  public OProperty createProperty(ODatabaseSession session, final String iPropertyName,
      final OType iType) {
    return delegate.createProperty(session, iPropertyName, iType);
  }

  @Override
  public OProperty createProperty(
      ODatabaseSession session, final String iPropertyName, final OType iType,
      final OClass iLinkedClass) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedClass);
  }

  @Override
  public OProperty createProperty(
      ODatabaseSession session, String iPropertyName, OType iType, OClass iLinkedClass,
      boolean unsafe) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedClass, unsafe);
  }

  @Override
  public OProperty createProperty(
      ODatabaseSession session, final String iPropertyName, final OType iType,
      final OType iLinkedType) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedType);
  }

  @Override
  public OProperty createProperty(
      ODatabaseSession session, String iPropertyName, OType iType, OType iLinkedType,
      boolean unsafe) {
    return delegate.createProperty(session, iPropertyName, iType, iLinkedType, unsafe);
  }

  @Override
  public void dropProperty(ODatabaseSession session, final String iPropertyName) {
    delegate.dropProperty(session, iPropertyName);
  }

  @Override
  public boolean existsProperty(final String iPropertyName) {
    return delegate.existsProperty(iPropertyName);
  }

  @Override
  public int getClusterForNewInstance(final ODocument doc) {
    return delegate.getClusterForNewInstance(doc);
  }

  @Override
  public int getDefaultClusterId() {
    return delegate.getDefaultClusterId();
  }

  @Override
  public void setDefaultClusterId(ODatabaseSession session, final int iDefaultClusterId) {
    delegate.setDefaultClusterId(session, iDefaultClusterId);
  }

  @Override
  public int[] getClusterIds() {
    return delegate.getClusterIds();
  }

  @Override
  public OClass addClusterId(ODatabaseSession session, final int iId) {
    delegate.addClusterId(session, iId);
    return this;
  }

  @Override
  public OClusterSelectionStrategy getClusterSelection() {
    return delegate.getClusterSelection();
  }

  @Override
  public OClass setClusterSelection(ODatabaseSession session,
      final OClusterSelectionStrategy clusterSelection) {
    delegate.setClusterSelection(session, clusterSelection);
    return this;
  }

  @Override
  public OClass setClusterSelection(ODatabaseSession session, final String iStrategyName) {
    delegate.setClusterSelection(session, iStrategyName);
    return this;
  }

  @Override
  public OClass addCluster(ODatabaseSession session, final String iClusterName) {
    delegate.addCluster(session, iClusterName);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OClass truncateCluster(ODatabaseSession session, String clusterName) {
    delegate.truncateCluster(session, clusterName);

    return this;
  }

  @Override
  public OClass removeClusterId(ODatabaseSession session, final int iId) {
    delegate.removeClusterId(session, iId);
    return this;
  }

  @Override
  public int[] getPolymorphicClusterIds() {
    return delegate.getPolymorphicClusterIds();
  }

  @Override
  public Collection<OClass> getSubclasses() {
    return delegate.getSubclasses();
  }

  @Override
  @Deprecated
  public Collection<OClass> getBaseClasses() {
    return delegate.getSubclasses();
  }

  @Override
  public Collection<OClass> getAllSubclasses() {
    return delegate.getAllSubclasses();
  }

  @Override
  public Collection<OClass> getAllSuperClasses() {
    return delegate.getAllSuperClasses();
  }

  @Override
  @Deprecated
  public Collection<OClass> getAllBaseClasses() {
    return delegate.getAllSubclasses();
  }

  @Override
  public long getSize(ODatabaseSessionInternal session) {
    return delegate.getSize(session);
  }

  @Override
  public float getOverSize() {
    return delegate.getOverSize();
  }

  @Override
  public OClass setOverSize(ODatabaseSession session, final float overSize) {
    delegate.setOverSize(session, overSize);
    return this;
  }

  @Override
  public long count(ODatabaseSession session) {
    return delegate.count(session);
  }

  @Override
  public long count(ODatabaseSession session, final boolean iPolymorphic) {
    return delegate.count(session, iPolymorphic);
  }

  @Override
  public void truncate(ODatabaseSession session) throws IOException {
    delegate.truncate(session);
  }

  @Override
  public boolean isSubClassOf(final String iClassName) {
    return delegate.isSubClassOf(iClassName);
  }

  @Override
  public boolean isSubClassOf(final OClass iClass) {
    return delegate.isSubClassOf(iClass);
  }

  @Override
  public boolean isSuperClassOf(final OClass iClass) {
    return delegate.isSuperClassOf(iClass);
  }

  @Override
  public String getShortName() {
    return delegate.getShortName();
  }

  @Override
  public OClass setShortName(ODatabaseSession session, final String shortName) {
    delegate.setShortName(session, shortName);
    return this;
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public OClass setDescription(ODatabaseSession session, String iDescription) {
    delegate.setDescription(session, iDescription);
    return this;
  }

  @Override
  public Object get(ATTRIBUTES iAttribute) {
    return delegate.get(iAttribute);
  }

  @Override
  public OClass set(ODatabaseSession session, ATTRIBUTES attribute, Object iValue) {
    delegate.set(session, attribute, iValue);
    return this;
  }

  @Override
  public OIndex createIndex(ODatabaseSession session, final String iName, final INDEX_TYPE iType,
      final String... fields) {
    return delegate.createIndex(session, iName, iType, fields);
  }

  @Override
  public OIndex createIndex(ODatabaseSession session, final String iName, final String iType,
      final String... fields) {
    return delegate.createIndex(session, iName, iType, fields);
  }

  @Override
  public OIndex createIndex(
      ODatabaseSession session, final String iName,
      final INDEX_TYPE iType,
      final OProgressListener iProgressListener,
      final String... fields) {
    return delegate.createIndex(session, iName, iType, iProgressListener, fields);
  }

  @Override
  public OIndex createIndex(
      ODatabaseSession session, final String iName,
      final String iType,
      final OProgressListener iProgressListener,
      final ODocument metadata,
      String algorithm,
      String... fields) {
    return delegate.createIndex(session, iName, iType, iProgressListener, metadata, algorithm,
        fields);
  }

  @Override
  public OIndex createIndex(
      ODatabaseSession session, final String iName,
      final String iType,
      final OProgressListener iProgressListener,
      final ODocument metadata,
      String... fields) {
    return delegate.createIndex(session, iName, iType, iProgressListener, metadata, fields);
  }

  @Override
  public Set<OIndex> getInvolvedIndexes(ODatabaseSession session, final Collection<String> fields) {
    return delegate.getInvolvedIndexes(session, fields);
  }

  @Override
  public Set<OIndex> getInvolvedIndexes(ODatabaseSession session, final String... fields) {
    return delegate.getInvolvedIndexes(session, fields);
  }

  @Override
  public Set<OIndex> getClassInvolvedIndexes(ODatabaseSession session,
      final Collection<String> fields) {
    return delegate.getClassInvolvedIndexes(session, fields);
  }

  @Override
  public Set<OIndex> getClassInvolvedIndexes(ODatabaseSession session, final String... fields) {
    return delegate.getClassInvolvedIndexes(session, fields);
  }

  @Override
  public boolean areIndexed(ODatabaseSession session, final Collection<String> fields) {
    return delegate.areIndexed(session, fields);
  }

  @Override
  public boolean areIndexed(ODatabaseSession session, final String... fields) {
    return delegate.areIndexed(session, fields);
  }

  @Override
  public OIndex getClassIndex(ODatabaseSession session, final String iName) {
    return delegate.getClassIndex(session, iName);
  }

  @Override
  public Set<OIndex> getClassIndexes(ODatabaseSession session) {
    return delegate.getClassIndexes(session);
  }

  @Override
  public void getClassIndexes(ODatabaseSession session, final Collection<OIndex> indexes) {
    delegate.getClassIndexes(session, indexes);
  }

  @Override
  public Set<OIndex> getIndexes(ODatabaseSession session) {
    return delegate.getIndexes(session);
  }

  @Override
  public String getCustom(final String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public OClass setCustom(ODatabaseSession session, final String iName, String iValue) {
    delegate.setCustom(session, iName, iValue);
    return this;
  }

  @Override
  public void removeCustom(ODatabaseSession session, final String iName) {
    delegate.removeCustom(session, iName);
  }

  @Override
  public void clearCustom(ODatabaseSession session) {
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
  public int compareTo(final OClass o) {
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

  public OClass getDelegate() {
    return delegate;
  }
}
