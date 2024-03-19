/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Document entry. Used by ODocument.
 *
 * @author Emanuele Tagliaferri
 * @since 2.1
 */
public class ODocumentEntry {

  private Object value;
  public Object original;
  public OType type;
  public OProperty property;
  private boolean changed = false;
  private boolean exists = true;
  private boolean created = false;
  private boolean txChanged = false;
  private boolean txExists = true;
  private boolean txCreated = false;

  public ODocumentEntry() {}

  public boolean isChanged() {
    return changed;
  }

  public void setChanged(final boolean changed) {
    this.changed = changed;
  }

  public boolean exists() {
    return exists;
  }

  public void setExists(final boolean exists) {
    this.exists = exists;
    this.txExists = exists;
  }

  public boolean isCreated() {
    return created;
  }

  protected ODocumentEntry clone() {
    final ODocumentEntry entry = new ODocumentEntry();
    entry.type = type;
    entry.property = property;
    entry.setValue(getValue());
    entry.changed = changed;
    entry.created = created;
    entry.exists = exists;
    entry.txChanged = changed;
    entry.txCreated = created;
    entry.txExists = exists;
    return entry;
  }

  public OMultiValueChangeTimeLine<Object, Object> getTimeLine() {
    if (!isChanged() && getValue() instanceof OTrackedMultiValue) {
      return ((OTrackedMultiValue) getValue()).getTimeLine();
    } else {
      return null;
    }
  }

  public void clear() {
    this.created = false;
    this.changed = false;
    original = null;
    removeTimeline();
  }

  public void clearNotExists() {
    original = null;
    removeTimeline();
  }

  public void removeTimeline() {
    if (getValue() instanceof OTrackedMultiValue) {
      ((OTrackedMultiValue) getValue()).disableTracking(null);
    }
  }

  public void replaceListener(ODocument document, Object oldValue) {
    enableTracking(document);
  }

  public boolean enableTracking(ODocument document) {
    if (getValue() instanceof OTrackedMultiValue) {
      ((OTrackedMultiValue) getValue()).enableTracking(document);
      return true;
    } else {
      return false;
    }
  }

  public void disableTracking(ODocument document, Object fieldValue) {
    if (fieldValue instanceof OTrackedMultiValue) {
      ((OTrackedMultiValue) fieldValue).disableTracking(document);
    }
  }

  public boolean isTrackedModified() {
    if (getValue() instanceof OTrackedMultiValue) {
      return ((OTrackedMultiValue) getValue()).isModified();
    }
    if (getValue() instanceof ODocument && ((ODocument) getValue()).isEmbedded()) {
      return ((ODocument) getValue()).isDirty();
    }
    return false;
  }

  public boolean isTxTrackedModified() {
    if (getValue() instanceof OTrackedMultiValue) {
      return ((OTrackedMultiValue) getValue()).isTransactionModified();
    }
    if (getValue() instanceof ODocument && ((ODocument) getValue()).isEmbedded()) {
      return ((ODocument) getValue()).isDirty();
    }
    return false;
  }

  public void markChanged() {
    this.changed = true;
    this.txChanged = true;
  }

  public void unmarkChanged() {
    this.changed = false;
  }

  public void markCreated() {
    this.created = true;
    this.txCreated = true;
  }

  public void unmarkCreated() {
    this.created = false;
  }

  public void undo() {
    if (isChanged()) {
      setValue(original);
      unmarkChanged();
      original = null;
      exists = true;
    }
  }

  public void transactionClear() {
    if (getValue() instanceof OTrackedMultiValue) {
      ((OTrackedMultiValue) getValue()).transactionClear();
    }
    this.txCreated = false;
    this.txChanged = false;
  }

  public boolean isTxChanged() {
    return txChanged;
  }

  public boolean isTxCreated() {
    return txCreated;
  }

  public boolean isTxExists() {
    return txExists;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    var prevValue = this.value;
    this.value = value;

    if (prevValue instanceof ODocumentEntryAware documentEntryAware) {
      documentEntryAware.clearDocumentEntry();
    }
    if (value instanceof ODocumentEntryAware documentEntryAware) {
      documentEntryAware.setDocumentEntry(this);
    }
    if (value instanceof ODocument document && document.isDirty()) {
      markChanged();
    }
  }
}
