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
package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string;

import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import java.io.StringWriter;

public class RecordSerializerJSON extends RecordSerializerStringAbstract {

  public static final String NAME = "json";

  @Override
  public <T extends DBRecord> T fromString(DatabaseSessionInternal session, String iContent,
      RecordAbstract iRecord, String[] iFields) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected StringWriter toString(DatabaseSessionInternal session, DBRecord iRecord,
      StringWriter iOutput, String iFormat, boolean autoDetectCollectionType) {
    throw new UnsupportedOperationException();
  }

}
