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
package com.orientechnologies.core.serialization.serializer.record;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTSerializationException;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.YTRecordAbstract;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.record.impl.YTRecordBytes;

public class ORecordSerializerRaw implements ORecordSerializer {

  public static final String NAME = "ORecordDocumentRaw";

  public YTRecord fromStream(final byte[] iSource) {
    return new YTRecordBytes(iSource);
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
  public String[] getFieldNames(YTDatabaseSessionInternal db, YTEntityImpl reference,
      byte[] iSource) {
    return null;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public YTRecordAbstract fromStream(
      YTDatabaseSessionInternal db, final byte[] iSource, final YTRecordAbstract iRecord,
      String[] iFields) {
    iRecord.reset();
    iRecord.fromStream(iSource);

    return iRecord;
  }

  @Override
  public byte[] toStream(YTDatabaseSessionInternal session, final YTRecordAbstract iSource) {
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
