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
package com.orientechnologies.orient.core.record;

import com.orientechnologies.common.exception.YTSystemException;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.record.impl.YTEdgeEntityImpl;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.record.impl.YTRecordBytes;
import com.orientechnologies.orient.core.record.impl.YTRecordFlat;
import com.orientechnologies.orient.core.record.impl.YTVertexEntityImpl;
import com.orientechnologies.orient.core.record.impl.YTViewEntityImpl;

/**
 * Record factory. To use your own record implementation use the declareRecordType() method. Example
 * of registration of the record MyRecord:
 *
 * <p><code>
 * declareRecordType('m', "myrecord", MyRecord.class);
 * </code>
 */
@SuppressWarnings("unchecked")
public class ORecordFactoryManager {

  protected final String[] recordTypeNames = new String[Byte.MAX_VALUE];
  protected final Class<? extends YTRecord>[] recordTypes = new Class[Byte.MAX_VALUE];
  protected final ORecordFactory[] recordFactories = new ORecordFactory[Byte.MAX_VALUE];

  public interface ORecordFactory {

    YTRecord newRecord(YTRID rid, YTDatabaseSessionInternal database);
  }

  public ORecordFactoryManager() {
    declareRecordType(
        YTEntityImpl.RECORD_TYPE,
        "document",
        YTEntityImpl.class,
        (rid, database) -> {
          var cluster = rid.getClusterId();
          if (database != null && cluster >= 0) {
            if (database.isClusterVertex(cluster)) {
              return new YTVertexEntityImpl(database, rid);
            } else if (database.isClusterEdge(cluster)) {
              return new YTEdgeEntityImpl(database, rid);
            } else if (database.isClusterView(cluster)) {
              return new YTViewEntityImpl(database, rid);
            }
          }
          return new YTEntityImpl(database, rid);
        });
    declareRecordType(
        YTBlob.RECORD_TYPE, "bytes", YTBlob.class, (rid, database) -> new YTRecordBytes(rid));
    declareRecordType(
        YTRecordFlat.RECORD_TYPE,
        "flat",
        YTRecordFlat.class,
        (rid, database) -> new YTRecordFlat(rid));
  }

  public String getRecordTypeName(final byte iRecordType) {
    String name = recordTypeNames[iRecordType];
    if (name == null) {
      throw new IllegalArgumentException("Unsupported record type: " + iRecordType);
    }
    return name;
  }

  public YTRecord newInstance(YTRID rid, YTDatabaseSessionInternal database) {
    try {
      return getFactory(database.getRecordType()).newRecord(rid, database);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unsupported record type: " + database.getRecordType(), e);
    }
  }

  public YTRecordAbstract newInstance(
      final byte iRecordType, YTRID rid, YTDatabaseSessionInternal database) {
    try {
      return (YTRecordAbstract) getFactory(iRecordType).newRecord(rid, database);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unsupported record type: " + iRecordType, e);
    }
  }

  public void declareRecordType(
      byte iByte, String iName, Class<? extends YTRecord> iClass, final ORecordFactory iFactory) {
    if (recordTypes[iByte] != null) {
      throw new YTSystemException(
          "Record type byte '" + iByte + "' already in use : " + recordTypes[iByte].getName());
    }
    recordTypeNames[iByte] = iName;
    recordTypes[iByte] = iClass;
    recordFactories[iByte] = iFactory;
  }

  protected ORecordFactory getFactory(final byte iRecordType) {
    final ORecordFactory factory = recordFactories[iRecordType];
    if (factory == null) {
      throw new IllegalArgumentException("Record type '" + iRecordType + "' is not supported");
    }
    return factory;
  }
}
