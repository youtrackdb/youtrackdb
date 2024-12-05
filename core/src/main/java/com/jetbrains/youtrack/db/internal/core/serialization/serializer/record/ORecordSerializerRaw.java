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

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTSerializationException;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.RecordBytes;

public class ORecordSerializerRaw implements ORecordSerializer {

  public static final String NAME = "ORecordDocumentRaw";

  public Record fromStream(final byte[] iSource) {
    return new RecordBytes(iSource);
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
  public String[] getFieldNames(YTDatabaseSessionInternal db, EntityImpl reference,
      byte[] iSource) {
    return null;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public RecordAbstract fromStream(
      YTDatabaseSessionInternal db, final byte[] iSource, final RecordAbstract iRecord,
      String[] iFields) {
    iRecord.reset();
    iRecord.fromStream(iSource);

    return iRecord;
  }

  @Override
  public byte[] toStream(YTDatabaseSessionInternal session, final RecordAbstract iSource) {
    try {
      return iSource.toStream();
    } catch (Exception e) {
      final String message =
          "Error on unmarshalling object in binary format: " + iSource.getIdentity();
      OLogManager.instance().error(this, message, e);
      throw YTException.wrapException(new YTSerializationException(message), e);
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
