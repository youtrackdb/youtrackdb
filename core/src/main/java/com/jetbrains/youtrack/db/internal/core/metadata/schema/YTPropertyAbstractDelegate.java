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

import com.jetbrains.youtrack.db.internal.core.collate.OCollate;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Abstract Delegate for YTProperty interface.
 */
public class YTPropertyAbstractDelegate implements YTProperty {

  protected final YTProperty delegate;

  public YTPropertyAbstractDelegate(final YTProperty delegate) {
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
  public YTProperty setName(YTDatabaseSession session, final String iName) {
    delegate.setName(session, iName);
    return this;
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public YTProperty setDescription(YTDatabaseSession session, String iDescription) {
    delegate.setDescription(session, iDescription);
    return this;
  }

  @Override
  public void set(YTDatabaseSession session, final ATTRIBUTES attribute, final Object iValue) {
    delegate.set(session, attribute, iValue);
  }

  @Override
  public YTType getType() {
    return delegate.getType();
  }

  @Override
  public YTClass getLinkedClass() {
    return delegate.getLinkedClass();
  }

  @Override
  public YTType getLinkedType() {
    return delegate.getLinkedType();
  }

  @Override
  public boolean isNotNull() {
    return delegate.isNotNull();
  }

  @Override
  public YTProperty setNotNull(YTDatabaseSession session, final boolean iNotNull) {
    delegate.setNotNull(session, iNotNull);
    return this;
  }

  @Override
  public OCollate getCollate() {
    return delegate.getCollate();
  }

  @Override
  public YTProperty setCollate(YTDatabaseSession session, final String iCollateName) {
    delegate.setCollate(session, iCollateName);
    return this;
  }

  @Override
  public boolean isMandatory() {
    return delegate.isMandatory();
  }

  @Override
  public YTProperty setMandatory(YTDatabaseSession session, final boolean mandatory) {
    delegate.setMandatory(session, mandatory);
    return this;
  }

  @Override
  public boolean isReadonly() {
    return delegate.isReadonly();
  }

  @Override
  public YTProperty setReadonly(YTDatabaseSession session, final boolean iReadonly) {
    delegate.setReadonly(session, iReadonly);
    return this;
  }

  @Override
  public String getMin() {
    return delegate.getMin();
  }

  @Override
  public YTProperty setMin(YTDatabaseSession session, final String min) {
    delegate.setMin(session, min);
    return this;
  }

  @Override
  public String getMax() {
    return delegate.getMax();
  }

  @Override
  public YTProperty setMax(YTDatabaseSession session, final String max) {
    delegate.setMax(session, max);
    return this;
  }

  @Override
  public String getDefaultValue() {
    return delegate.getDefaultValue();
  }

  @Override
  public YTProperty setDefaultValue(YTDatabaseSession session, final String defaultValue) {
    delegate.setDefaultValue(session, defaultValue);
    return this;
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, final INDEX_TYPE iType) {
    return delegate.createIndex(session, iType);
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, final String iType) {
    return delegate.createIndex(session, iType);
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, String iType, EntityImpl metadata) {
    return delegate.createIndex(session, iType, metadata);
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, INDEX_TYPE iType, EntityImpl metadata) {
    return delegate.createIndex(session, iType, metadata);
  }

  @Override
  public YTProperty setLinkedClass(YTDatabaseSession session, YTClass oClass) {
    delegate.setLinkedClass(session, oClass);
    return this;
  }

  @Override
  public YTProperty setLinkedType(YTDatabaseSession session, YTType type) {
    delegate.setLinkedType(session, type);
    return this;
  }

  @Override
  public YTProperty setCollate(YTDatabaseSession session, OCollate collate) {
    delegate.setCollate(session, collate);
    return this;
  }

  @Override
  @Deprecated
  public YTProperty dropIndexes(YTDatabaseSessionInternal session) {
    delegate.dropIndexes(session);
    return this;
  }

  @Override
  @Deprecated
  public Set<OIndex> getIndexes(YTDatabaseSession session) {
    return delegate.getIndexes(session);
  }

  @Override
  @Deprecated
  public OIndex getIndex(YTDatabaseSession session) {
    return delegate.getIndex(session);
  }

  @Override
  public Collection<OIndex> getAllIndexes(YTDatabaseSession session) {
    return delegate.getAllIndexes(session);
  }

  @Override
  @Deprecated
  public boolean isIndexed(YTDatabaseSession session) {
    return delegate.isIndexed(session);
  }

  @Override
  public String getRegexp() {
    return delegate.getRegexp();
  }

  @Override
  public YTProperty setRegexp(YTDatabaseSession session, final String regexp) {
    delegate.setRegexp(session, regexp);
    return this;
  }

  @Override
  public YTProperty setType(YTDatabaseSession session, final YTType iType) {
    delegate.setType(session, iType);
    return this;
  }

  @Override
  public String getCustom(final String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public YTProperty setCustom(YTDatabaseSession session, final String iName, final String iValue) {
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
  public YTClass getOwnerClass() {
    return delegate.getOwnerClass();
  }

  @Override
  public Object get(final ATTRIBUTES iAttribute) {
    return delegate.get(iAttribute);
  }

  @Override
  public int compareTo(@Nonnull final YTProperty o) {
    return delegate.compareTo(o);
  }
}
