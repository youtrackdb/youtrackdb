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

import com.jetbrains.youtrack.db.internal.core.collate.Collate;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Abstract Delegate for Property interface.
 */
public class PropertyAbstractDelegate implements Property {

  protected final Property delegate;

  public PropertyAbstractDelegate(final Property delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public Integer getId() {
    return delegate.getId();
  }

  @Override
  public String getFullName() {
    return delegate.getFullName();
  }

  @Override
  public Property setName(DatabaseSession session, final String iName) {
    delegate.setName(session, iName);
    return this;
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public Property setDescription(DatabaseSession session, String iDescription) {
    delegate.setDescription(session, iDescription);
    return this;
  }

  @Override
  public void set(DatabaseSession session, final ATTRIBUTES attribute, final Object iValue) {
    delegate.set(session, attribute, iValue);
  }

  @Override
  public PropertyType getType() {
    return delegate.getType();
  }

  @Override
  public SchemaClass getLinkedClass() {
    return delegate.getLinkedClass();
  }

  @Override
  public PropertyType getLinkedType() {
    return delegate.getLinkedType();
  }

  @Override
  public boolean isNotNull() {
    return delegate.isNotNull();
  }

  @Override
  public Property setNotNull(DatabaseSession session, final boolean iNotNull) {
    delegate.setNotNull(session, iNotNull);
    return this;
  }

  @Override
  public Collate getCollate() {
    return delegate.getCollate();
  }

  @Override
  public Property setCollate(DatabaseSession session, final String iCollateName) {
    delegate.setCollate(session, iCollateName);
    return this;
  }

  @Override
  public boolean isMandatory() {
    return delegate.isMandatory();
  }

  @Override
  public Property setMandatory(DatabaseSession session, final boolean mandatory) {
    delegate.setMandatory(session, mandatory);
    return this;
  }

  @Override
  public boolean isReadonly() {
    return delegate.isReadonly();
  }

  @Override
  public Property setReadonly(DatabaseSession session, final boolean iReadonly) {
    delegate.setReadonly(session, iReadonly);
    return this;
  }

  @Override
  public String getMin() {
    return delegate.getMin();
  }

  @Override
  public Property setMin(DatabaseSession session, final String min) {
    delegate.setMin(session, min);
    return this;
  }

  @Override
  public String getMax() {
    return delegate.getMax();
  }

  @Override
  public Property setMax(DatabaseSession session, final String max) {
    delegate.setMax(session, max);
    return this;
  }

  @Override
  public String getDefaultValue() {
    return delegate.getDefaultValue();
  }

  @Override
  public Property setDefaultValue(DatabaseSession session, final String defaultValue) {
    delegate.setDefaultValue(session, defaultValue);
    return this;
  }

  @Override
  public Index createIndex(DatabaseSession session, final INDEX_TYPE iType) {
    return delegate.createIndex(session, iType);
  }

  @Override
  public Index createIndex(DatabaseSession session, final String iType) {
    return delegate.createIndex(session, iType);
  }

  @Override
  public Index createIndex(DatabaseSession session, String iType, EntityImpl metadata) {
    return delegate.createIndex(session, iType, metadata);
  }

  @Override
  public Index createIndex(DatabaseSession session, INDEX_TYPE iType, EntityImpl metadata) {
    return delegate.createIndex(session, iType, metadata);
  }

  @Override
  public Property setLinkedClass(DatabaseSession session, SchemaClass oClass) {
    delegate.setLinkedClass(session, oClass);
    return this;
  }

  @Override
  public Property setLinkedType(DatabaseSession session, PropertyType type) {
    delegate.setLinkedType(session, type);
    return this;
  }

  @Override
  public Property setCollate(DatabaseSession session, Collate collate) {
    delegate.setCollate(session, collate);
    return this;
  }

  @Override
  @Deprecated
  public Property dropIndexes(DatabaseSessionInternal session) {
    delegate.dropIndexes(session);
    return this;
  }

  @Override
  @Deprecated
  public Set<Index> getIndexes(DatabaseSession session) {
    return delegate.getIndexes(session);
  }

  @Override
  @Deprecated
  public Index getIndex(DatabaseSession session) {
    return delegate.getIndex(session);
  }

  @Override
  public Collection<Index> getAllIndexes(DatabaseSession session) {
    return delegate.getAllIndexes(session);
  }

  @Override
  @Deprecated
  public boolean isIndexed(DatabaseSession session) {
    return delegate.isIndexed(session);
  }

  @Override
  public String getRegexp() {
    return delegate.getRegexp();
  }

  @Override
  public Property setRegexp(DatabaseSession session, final String regexp) {
    delegate.setRegexp(session, regexp);
    return this;
  }

  @Override
  public Property setType(DatabaseSession session, final PropertyType iType) {
    delegate.setType(session, iType);
    return this;
  }

  @Override
  public String getCustom(final String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public Property setCustom(DatabaseSession session, final String iName, final String iValue) {
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
  public SchemaClass getOwnerClass() {
    return delegate.getOwnerClass();
  }

  @Override
  public Object get(final ATTRIBUTES iAttribute) {
    return delegate.get(iAttribute);
  }

  @Override
  public int compareTo(@Nonnull final Property o) {
    return delegate.compareTo(o);
  }
}
