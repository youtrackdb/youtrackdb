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
package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import java.io.Serial;
import java.util.Objects;

public class RecordNotFoundException extends CoreException implements HighLevelException {

  @Serial
  private static final long serialVersionUID = -265573123216968L;

  private final RID rid;

  public RecordNotFoundException(final RecordNotFoundException exception) {
    super(exception);
    this.rid = exception.rid;
  }

  public RecordNotFoundException(final RID iRID) {
    super("The record with id '" + iRID + "' was not found");
    rid = iRID;
  }

  public RecordNotFoundException(final RID iRID, final String message) {
    super(message);
    rid = iRID;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof RecordNotFoundException)) {
      return false;
    }

    if (rid == null && ((RecordNotFoundException) obj).rid == null) {
      return toString().equals(obj.toString());
    }

    return rid != null && rid.equals(((RecordNotFoundException) obj).rid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rid);
  }

  public RID getRid() {
    return rid;
  }
}
