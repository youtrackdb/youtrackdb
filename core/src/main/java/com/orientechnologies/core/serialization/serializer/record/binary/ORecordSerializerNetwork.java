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

package com.orientechnologies.core.serialization.serializer.record.binary;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.metadata.schema.YTImmutableSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.YTRecordAbstract;
import com.orientechnologies.core.record.impl.YTBlob;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.record.impl.YTRecordFlat;
import com.orientechnologies.core.serialization.serializer.record.ORecordSerializer;
import java.util.Base64;

public class ORecordSerializerNetwork implements ORecordSerializer {

  public static final String NAME = "onet_ser_v0";
  public static final ORecordSerializerNetwork INSTANCE = new ORecordSerializerNetwork();
  private static final byte CURRENT_RECORD_VERSION = 0;

  private final ODocumentSerializer[] serializerByVersion;

  public ORecordSerializerNetwork() {
    serializerByVersion = new ODocumentSerializer[1];
    serializerByVersion[0] = new ORecordSerializerNetworkV0();
  }

  @Override
  public int getCurrentVersion() {
    return CURRENT_RECORD_VERSION;
  }

  @Override
  public int getMinSupportedVersion() {
    return CURRENT_RECORD_VERSION;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public YTRecordAbstract fromStream(
      YTDatabaseSessionInternal db, final byte[] iSource, YTRecordAbstract iRecord,
      final String[] iFields) {
    if (iSource == null || iSource.length == 0) {
      return iRecord;
    }
    if (iRecord == null) {
      iRecord = new YTEntityImpl();
    } else if (iRecord instanceof YTBlob) {
      iRecord.fromStream(iSource);
      return iRecord;
    } else if (iRecord instanceof YTRecordFlat) {
      iRecord.fromStream(iSource);
      return iRecord;
    }

    BytesContainer container = new BytesContainer(iSource);
    container.skip(1);

    try {
      if (iFields != null && iFields.length > 0) {
        serializerByVersion[iSource[0]].deserializePartial(db, (YTEntityImpl) iRecord, container,
            iFields);
      } else {
        serializerByVersion[iSource[0]].deserialize(db, (YTEntityImpl) iRecord, container);
      }
    } catch (RuntimeException e) {
      OLogManager.instance()
          .warn(
              this,
              "Error deserializing record with id %s send this data for debugging: %s ",
              iRecord.getIdentity().toString(),
              Base64.getEncoder().encodeToString(iSource));
      throw e;
    }
    return iRecord;
  }

  @Override
  public byte[] toStream(YTDatabaseSessionInternal session, YTRecordAbstract iSource) {
    if (iSource instanceof YTBlob) {
      return iSource.toStream();
    } else if (iSource instanceof YTRecordFlat) {
      return iSource.toStream();
    } else {
      final BytesContainer container = new BytesContainer();

      // WRITE SERIALIZER VERSION
      int pos = container.alloc(1);
      container.bytes[pos] = CURRENT_RECORD_VERSION;
      // SERIALIZE RECORD
      serializerByVersion[CURRENT_RECORD_VERSION].serialize(session, (YTEntityImpl) iSource,
          container);

      return container.fitBytes();
    }
  }

  public byte[] serializeValue(YTDatabaseSessionInternal db, Object value, YTType type) {
    YTImmutableSchema schema = null;
    if (db != null) {
      schema = db.getMetadata().getImmutableSchemaSnapshot();
    }
    BytesContainer bytes = new BytesContainer();
    serializerByVersion[0].serializeValue(db, bytes, value, type, null, schema, null);
    return bytes.fitBytes();
  }

  public Object deserializeValue(YTDatabaseSessionInternal db, byte[] val, YTType type) {
    BytesContainer bytes = new BytesContainer(val);
    return serializerByVersion[0].deserializeValue(db, bytes, type, null);
  }

  @Override
  public boolean getSupportBinaryEvaluate() {
    return false;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String[] getFieldNames(YTDatabaseSessionInternal db, YTEntityImpl reference,
      byte[] iSource) {
    if (iSource == null || iSource.length == 0) {
      return new String[0];
    }

    final BytesContainer container = new BytesContainer(iSource).skip(1);

    try {
      return serializerByVersion[iSource[0]].getFieldNames(reference, container, false);
    } catch (RuntimeException e) {
      OLogManager.instance()
          .warn(
              this,
              "Error deserializing record to get field-names, send this data for debugging: %s ",
              Base64.getEncoder().encodeToString(iSource));
      throw e;
    }
  }
}
