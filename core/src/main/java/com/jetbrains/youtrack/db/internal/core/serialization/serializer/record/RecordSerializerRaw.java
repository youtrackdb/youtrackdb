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
package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes;

public class RecordSerializerRaw implements RecordSerializer {

  public static final String NAME = "ORecordDocumentRaw";

  public static Record fromStream(DatabaseSessionInternal db, final byte[] iSource) {
    return new RecordBytes(db, iSource);
  }

  @Override
  public int getCurrentVersion() {
    return 0;
  }

  @Override
  public int getMinSupportedVersion() {
    return 0;
  }

  @Override
  public String[] getFieldNames(DatabaseSessionInternal db, EntityImpl reference,
      byte[] iSource) {
    return null;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public RecordAbstract fromStream(
      DatabaseSessionInternal db, final byte[] iSource, final RecordAbstract iRecord,
      String[] iFields) {
    iRecord.reset();
    iRecord.fromStream(iSource);

    return iRecord;
  }

  @Override
  public byte[] toStream(DatabaseSessionInternal db, final RecordAbstract iSource) {
    try {
      return iSource.toStream();
    } catch (Exception e) {
      final String message =
          "Error on unmarshalling object in binary format: " + iSource.getIdentity();
      LogManager.instance().error(this, message, e);
      throw BaseException.wrapException(new SerializationException(message), e);
    }
  }

  @Override
  public boolean getSupportBinaryEvaluate() {
    return false;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
