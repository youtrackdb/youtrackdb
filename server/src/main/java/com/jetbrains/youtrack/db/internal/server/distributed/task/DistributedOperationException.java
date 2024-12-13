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
package com.jetbrains.youtrack.db.internal.server.distributed.task;

import com.jetbrains.youtrack.db.api.exception.HighLevelException;
import com.jetbrains.youtrack.db.internal.common.concur.NeedRetryException;

/**
 * Exception thrown when a distributed operation doesn't reach the quorum.
 */
public class DistributedOperationException extends NeedRetryException
    implements HighLevelException {

  private static final long serialVersionUID = 1L;

  public DistributedOperationException(final DistributedOperationException exception) {
    super(exception);
  }

  public DistributedOperationException(final String message) {
    super(message);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !obj.getClass().equals(getClass())) {
      return false;
    }

    final String message = ((DistributedOperationException) obj).getMessage();
    return getMessage() != null && getMessage().equals(message);
  }

  @Override
  public int hashCode() {
    return getMessage() != null ? getMessage().hashCode() : 0;
  }
}
