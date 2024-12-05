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

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.record.YTRecordAbstract;
import com.orientechnologies.core.record.impl.YTEntityImpl;

public interface ORecordSerializer {

  YTRecordAbstract fromStream(YTDatabaseSessionInternal db, byte[] iSource,
      YTRecordAbstract iRecord,
      String[] iFields);

  byte[] toStream(YTDatabaseSessionInternal session, YTRecordAbstract iSource);

  int getCurrentVersion();

  int getMinSupportedVersion();

  String[] getFieldNames(YTDatabaseSessionInternal db, YTEntityImpl reference, byte[] iSource);

  boolean getSupportBinaryEvaluate();

  String getName();
}
