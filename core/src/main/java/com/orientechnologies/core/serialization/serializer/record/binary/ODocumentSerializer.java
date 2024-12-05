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

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.RecordElement;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTImmutableSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.metadata.security.PropertyEncryption;
import com.orientechnologies.core.record.impl.YTEntityImpl;

public interface ODocumentSerializer {

  void serialize(YTDatabaseSessionInternal session, YTEntityImpl document, BytesContainer bytes);

  int serializeValue(
      YTDatabaseSessionInternal session, BytesContainer bytes,
      Object value,
      YTType type,
      YTType linkedType,
      YTImmutableSchema schema,
      PropertyEncryption encryption);

  void deserialize(YTDatabaseSessionInternal db, YTEntityImpl document, BytesContainer bytes);

  void deserializePartial(YTDatabaseSessionInternal db, YTEntityImpl document, BytesContainer bytes,
      String[] iFields);

  Object deserializeValue(YTDatabaseSessionInternal session, BytesContainer bytes, YTType type,
      RecordElement owner);

  OBinaryField deserializeField(
      BytesContainer bytes,
      YTClass iClass,
      String iFieldName,
      boolean embedded,
      YTImmutableSchema schema,
      PropertyEncryption encryption);

  OBinaryComparator getComparator();

  /**
   * Returns the array of field names with no values.
   *
   * @param reference TODO
   * @param embedded
   */
  String[] getFieldNames(YTEntityImpl reference, BytesContainer iBytes, boolean embedded);

  boolean isSerializingClassNameByDefault();

  <RET> RET deserializeFieldTyped(
      YTDatabaseSessionInternal session, BytesContainer record,
      String iFieldName,
      boolean isEmbedded,
      YTImmutableSchema schema,
      PropertyEncryption encryption);

  void deserializeDebug(
      YTDatabaseSessionInternal db, BytesContainer bytes,
      ORecordSerializationDebug debugInfo,
      YTImmutableSchema schema);
}
