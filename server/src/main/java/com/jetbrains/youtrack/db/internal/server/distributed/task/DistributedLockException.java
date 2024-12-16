/*
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

/**
 * Exception thrown when a distributed resource is locked.
 */
public class DistributedLockException extends DistributedOperationException
    implements HighLevelException {

  public DistributedLockException(final DistributedLockException exception) {
    super(exception);
  }

  public DistributedLockException(final String message) {
    super(message);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !obj.getClass().equals(getClass())) {
      return false;
    }

    final String message = ((DistributedLockException) obj).getMessage();
    return getMessage() != null && getMessage().equals(message);
  }
}
