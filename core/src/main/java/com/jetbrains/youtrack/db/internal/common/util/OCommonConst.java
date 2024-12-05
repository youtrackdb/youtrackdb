/*
 *
 *  *  Copyright YouTrackDB
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
package com.jetbrains.youtrack.db.internal.common.util;

import com.jetbrains.youtrack.db.internal.core.config.OStorageFileConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.storage.OCluster;
import com.jetbrains.youtrack.db.internal.core.storage.OPhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.cache.OPageDataVerificationError;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.OHashTable;

public final class OCommonConst {

  public static final long[] EMPTY_LONG_ARRAY = new long[0];
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  public static final char[] EMPTY_CHAR_ARRAY = new char[0];
  public static final int[] EMPTY_INT_ARRAY = new int[0];
  public static final OCluster[] EMPTY_CLUSTER_ARRAY = new OCluster[0];
  public static final YTIdentifiable[] EMPTY_IDENTIFIABLE_ARRAY = new YTIdentifiable[0];
  public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
  public static final YTType[] EMPTY_TYPES_ARRAY = new YTType[0];
  public static final OPageDataVerificationError[] EMPTY_PAGE_DATA_VERIFICATION_ARRAY =
      new OPageDataVerificationError[0];
  public static final OHashTable.Entry[] EMPTY_BUCKET_ENTRY_ARRAY = new OHashTable.Entry[0];
  public static final OPhysicalPosition[] EMPTY_PHYSICAL_POSITIONS_ARRAY = new OPhysicalPosition[0];
  public static final OStorageFileConfiguration[] EMPTY_FILE_CONFIGURATIONS_ARRAY =
      new OStorageFileConfiguration[0];

  private OCommonConst() {
  }
}
