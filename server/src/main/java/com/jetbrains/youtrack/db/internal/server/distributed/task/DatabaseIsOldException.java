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

import com.jetbrains.youtrack.db.internal.server.distributed.DistributedException;

/**
 * Exception thrown when a database is requested but it is older then the one owned by the
 * requester.
 */
public class DatabaseIsOldException extends DistributedException {

  public DatabaseIsOldException(final DatabaseIsOldException exception) {
    super(exception);
  }

  public DatabaseIsOldException(final String iMessage) {
    super(iMessage);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DatabaseIsOldException)) {
      return false;
    }

    return getMessage().equals(((DatabaseIsOldException) obj).getMessage());
  }
}
