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
package com.jetbrains.youtrack.db.internal.core.iterator;

import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

/**
 * Record iterator to browse records in inverse order: from last to the first.
 */
public class RecordIteratorClassDescendentOrder<REC extends DBRecord>
    extends RecordIteratorClass<REC> {

  public RecordIteratorClassDescendentOrder(
      DatabaseSessionInternal iDatabase,
      DatabaseSessionInternal iLowLevelDatabase,
      String iClassName,
      boolean iPolymorphic) {
    super(iDatabase, iClassName, iPolymorphic);

    currentClusterIdx = clusterIds.length - 1; // START FROM THE LAST CLUSTER
    updateClusterRange();
  }

  @Override
  public RecordIteratorClusters<REC> begin() {
    return super.last();
  }

  @Override
  public REC next() {
    return super.previous();
  }

  @Override
  public boolean hasNext() {
    return super.hasPrevious();
  }
}
