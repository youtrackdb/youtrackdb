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

package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTImmutableSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.metadata.security.PropertyEncryption;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

public interface ODocumentSerializer {

  void serialize(YTDatabaseSessionInternal session, EntityImpl document, BytesContainer bytes);

  int serializeValue(
      YTDatabaseSessionInternal session, BytesContainer bytes,
      Object value,
      YTType type,
      YTType linkedType,
      YTImmutableSchema schema,
      PropertyEncryption encryption);

  void deserialize(YTDatabaseSessionInternal db, EntityImpl document, BytesContainer bytes);

  void deserializePartial(YTDatabaseSessionInternal db, EntityImpl document, BytesContainer bytes,
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
  String[] getFieldNames(EntityImpl reference, BytesContainer iBytes, boolean embedded);

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
