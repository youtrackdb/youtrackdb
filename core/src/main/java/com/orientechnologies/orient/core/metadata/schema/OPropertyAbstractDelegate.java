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

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Abstract Delegate for OProperty interface.
 */
public class OPropertyAbstractDelegate implements OProperty {

  protected final OProperty delegate;

  public OPropertyAbstractDelegate(final OProperty delegate) {
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
  public OProperty setName(ODatabaseSession session, final String iName) {
    delegate.setName(session, iName);
    return this;
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public OProperty setDescription(ODatabaseSession session, String iDescription) {
    delegate.setDescription(session, iDescription);
    return this;
  }

  @Override
  public void set(ODatabaseSession session, final ATTRIBUTES attribute, final Object iValue) {
    delegate.set(session, attribute, iValue);
  }

  @Override
  public OType getType() {
    return delegate.getType();
  }

  @Override
  public OClass getLinkedClass() {
    return delegate.getLinkedClass();
  }

  @Override
  public OType getLinkedType() {
    return delegate.getLinkedType();
  }

  @Override
  public boolean isNotNull() {
    return delegate.isNotNull();
  }

  @Override
  public OProperty setNotNull(ODatabaseSession session, final boolean iNotNull) {
    delegate.setNotNull(session, iNotNull);
    return this;
  }

  @Override
  public OCollate getCollate() {
    return delegate.getCollate();
  }

  @Override
  public OProperty setCollate(ODatabaseSession session, final String iCollateName) {
    delegate.setCollate(session, iCollateName);
    return this;
  }

  @Override
  public boolean isMandatory() {
    return delegate.isMandatory();
  }

  @Override
  public OProperty setMandatory(ODatabaseSession session, final boolean mandatory) {
    delegate.setMandatory(session, mandatory);
    return this;
  }

  @Override
  public boolean isReadonly() {
    return delegate.isReadonly();
  }

  @Override
  public OProperty setReadonly(ODatabaseSession session, final boolean iReadonly) {
    delegate.setReadonly(session, iReadonly);
    return this;
  }

  @Override
  public String getMin() {
    return delegate.getMin();
  }

  @Override
  public OProperty setMin(ODatabaseSession session, final String min) {
    delegate.setMin(session, min);
    return this;
  }

  @Override
  public String getMax() {
    return delegate.getMax();
  }

  @Override
  public OProperty setMax(ODatabaseSession session, final String max) {
    delegate.setMax(session, max);
    return this;
  }

  @Override
  public String getDefaultValue() {
    return delegate.getDefaultValue();
  }

  @Override
  public OProperty setDefaultValue(ODatabaseSession session, final String defaultValue) {
    delegate.setDefaultValue(session, defaultValue);
    return this;
  }

  @Override
  public OIndex createIndex(ODatabaseSession session, final INDEX_TYPE iType) {
    return delegate.createIndex(session, iType);
  }

  @Override
  public OIndex createIndex(ODatabaseSession session, final String iType) {
    return delegate.createIndex(session, iType);
  }

  @Override
  public OIndex createIndex(ODatabaseSession session, String iType, ODocument metadata) {
    return delegate.createIndex(iType, metadata);
  }

  @Override
  public OIndex createIndex(OClass.INDEX_TYPE iType, ODocument metadata) {
    return delegate.createIndex(iType, metadata);
  }

  @Override
  public OProperty setLinkedClass(ODatabaseSession session, OClass oClass) {
    delegate.setLinkedClass(session, oClass);
    return this;
  }

  @Override
  public OProperty setLinkedType(ODatabaseSession session, OType type) {
    delegate.setLinkedType(session, type);
    return this;
  }

  @Override
  public OProperty setCollate(ODatabaseSession session, OCollate collate) {
    delegate.setCollate(session, collate);
    return this;
  }

  @Override
  @Deprecated
  public OProperty dropIndexes(ODatabaseSessionInternal session) {
    delegate.dropIndexes(session);
    return this;
  }

  @Override
  @Deprecated
  public Set<OIndex> getIndexes(ODatabaseSession session) {
    return delegate.getIndexes(session);
  }

  @Override
  @Deprecated
  public OIndex getIndex(ODatabaseSession session) {
    return delegate.getIndex(session);
  }

  @Override
  public Collection<OIndex> getAllIndexes(ODatabaseSession session) {
    return delegate.getAllIndexes(session);
  }

  @Override
  @Deprecated
  public boolean isIndexed(ODatabaseSession session) {
    return delegate.isIndexed(session);
  }

  @Override
  public String getRegexp() {
    return delegate.getRegexp();
  }

  @Override
  public OProperty setRegexp(ODatabaseSession session, final String regexp) {
    delegate.setRegexp(session, regexp);
    return this;
  }

  @Override
  public OProperty setType(ODatabaseSession session, final OType iType) {
    delegate.setType(session, iType);
    return this;
  }

  @Override
  public String getCustom(final String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public OProperty setCustom(ODatabaseSession session, final String iName, final String iValue) {
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
  public OClass getOwnerClass() {
    return delegate.getOwnerClass();
  }

  @Override
  public Object get(final ATTRIBUTES iAttribute) {
    return delegate.get(iAttribute);
  }

  @Override
  public int compareTo(@Nonnull final OProperty o) {
    return delegate.compareTo(o);
  }
}
