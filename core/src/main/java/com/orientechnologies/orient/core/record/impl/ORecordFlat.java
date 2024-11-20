/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordStringable;
import java.nio.charset.StandardCharsets;

/**
 * It's schema less. Use this if you need to store Strings at low level. The object can be reused
 * across calls to the database by using the reset() at every re-use.
 */
@SuppressWarnings({"unchecked"})
@Deprecated
public class ORecordFlat extends ORecordAbstract implements ORecordStringable {

  private static final long serialVersionUID = 1L;
  public static final byte RECORD_TYPE = 'f';
  protected String value;

  public ORecordFlat(ODatabaseSessionInternal iDatabase) {
    this();
    ODatabaseRecordThreadLocal.instance().set(iDatabase);
  }

  public ORecordFlat(ORID rid) {
    recordId = (ORecordId) rid.copy();
  }

  public ORecordFlat() {
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }

  public ORecordFlat value(final String iValue) {
    value = iValue;
    setDirty();
    return this;
  }

  @Override
  public ORecordFlat reset() {
    super.reset();
    value = null;
    return this;
  }

  @Override
  public ORecordFlat unload() {
    super.unload();
    value = null;
    return this;
  }

  @Override
  public ORecordFlat clear() {
    super.clear();
    value = null;
    return this;
  }

  public ORecordFlat copy() {
    ORecordFlat cloned = new ORecordFlat();
    cloned.source = source;
    cloned.value = value;
    cloned.recordId = recordId.copy();
    cloned.dirty = dirty;
    cloned.contentChanged = contentChanged;
    cloned.recordVersion = recordVersion;
    return cloned;
  }

  public String value() {
    if (value == null) {
      // LAZY LOADING: LOAD THE RECORD FIRST
      value = new String(source, StandardCharsets.UTF_8);
    }

    return value;
  }

  @Override
  public String toString() {
    return super.toString() + " " + value();
  }

  @Override
  public ORecordAbstract fromStream(final byte[] iRecordBuffer) {
    super.fromStream(iRecordBuffer);
    value = null;
    return this;
  }

  @Override
  public byte[] toStream() {
    if (source == null && value != null) {
      source = value.getBytes(StandardCharsets.UTF_8);
    }
    return source;
  }

  public int size() {
    final String v = value();
    return v != null ? v.length() : 0;
  }

  public byte getRecordType() {
    return RECORD_TYPE;
  }
}
