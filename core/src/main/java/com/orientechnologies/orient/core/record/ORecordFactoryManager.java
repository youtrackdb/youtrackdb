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
package com.orientechnologies.orient.core.record;

import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.OEdgeDocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.record.impl.OVertexDocument;
import com.orientechnologies.orient.core.record.impl.OViewDocument;

/**
 * Record factory. To use your own record implementation use the declareRecordType() method. Example
 * of registration of the record MyRecord:
 *
 * <p><code>
 * declareRecordType('m', "myrecord", MyRecord.class);
 * </code>
 *
 * @author Sylvain Spinelli
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public class ORecordFactoryManager {

  protected final String[] recordTypeNames = new String[Byte.MAX_VALUE];
  protected final Class<? extends ORecord>[] recordTypes = new Class[Byte.MAX_VALUE];
  protected final ORecordFactory[] recordFactories = new ORecordFactory[Byte.MAX_VALUE];

  public interface ORecordFactory {

    ORecord newRecord(ORID rid, ODatabaseSessionInternal database);
  }

  public ORecordFactoryManager() {
    declareRecordType(
        ODocument.RECORD_TYPE,
        "document",
        ODocument.class,
        (rid, database) -> {
          var cluster = rid.getClusterId();
          if (database != null && cluster >= 0) {
            if (database.isClusterVertex(cluster)) {
              return new OVertexDocument(database, rid);
            } else if (database.isClusterEdge(cluster)) {
              return new OEdgeDocument(database, rid);
            } else if (database.isClusterView(cluster)) {
              return new OViewDocument(database, rid);
            }
          }
          return new ODocument(database, rid);
        });
    declareRecordType(
        OBlob.RECORD_TYPE, "bytes", OBlob.class, (rid, database) -> new ORecordBytes(rid));
    declareRecordType(
        ORecordFlat.RECORD_TYPE,
        "flat",
        ORecordFlat.class,
        (rid, database) -> new ORecordFlat(rid));
  }

  public String getRecordTypeName(final byte iRecordType) {
    String name = recordTypeNames[iRecordType];
    if (name == null) {
      throw new IllegalArgumentException("Unsupported record type: " + iRecordType);
    }
    return name;
  }

  public ORecord newInstance(ORID rid, ODatabaseSessionInternal database) {
    try {
      return getFactory(database.getRecordType()).newRecord(rid, database);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unsupported record type: " + database.getRecordType(), e);
    }
  }

  public ORecordAbstract newInstance(
      final byte iRecordType, ORID rid, ODatabaseSessionInternal database) {
    try {
      return (ORecordAbstract) getFactory(iRecordType).newRecord(rid, database);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unsupported record type: " + iRecordType, e);
    }
  }

  public void declareRecordType(
      byte iByte, String iName, Class<? extends ORecord> iClass, final ORecordFactory iFactory) {
    if (recordTypes[iByte] != null) {
      throw new OSystemException(
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
