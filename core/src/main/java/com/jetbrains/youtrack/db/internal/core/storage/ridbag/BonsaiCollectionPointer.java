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

package com.jetbrains.youtrack.db.internal.core.storage.ridbag;

import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.ridbagbtree.RidBagBucketPointer;

/**
 * The pointer to a bonsai collection.
 *
 * <p>Determines where the collection is stored. Contains file id and pointer to the root bucket.
 * Is immutable.
 *
 * @see RidBag
 * @since 1.7rc1
 */
public class BonsaiCollectionPointer {

  public static final BonsaiCollectionPointer INVALID =
      new BonsaiCollectionPointer(-1, new RidBagBucketPointer(-1, -1));

  private final long fileId;
  private final RidBagBucketPointer rootPointer;

  public BonsaiCollectionPointer(long fileId, RidBagBucketPointer rootPointer) {
    this.fileId = fileId;
    this.rootPointer = rootPointer;
  }

  public long getFileId() {
    return fileId;
  }

  public RidBagBucketPointer getRootPointer() {
    return rootPointer;
  }

  public boolean isValid() {
    return fileId >= 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BonsaiCollectionPointer that = (BonsaiCollectionPointer) o;

    if (fileId != that.fileId) {
      return false;
    }
    return rootPointer.equals(that.rootPointer);
  }

  @Override
  public int hashCode() {
    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + rootPointer.hashCode();
    return result;
  }
}
