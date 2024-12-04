/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.exception.YTHighLevelException;

/**
 * Exception thrown when a distributed resource is locked.
 */
public class YTDistributedLockException extends YTDistributedOperationException
    implements YTHighLevelException {

  public YTDistributedLockException(final YTDistributedLockException exception) {
    super(exception);
  }

  public YTDistributedLockException(final String message) {
    super(message);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !obj.getClass().equals(getClass())) {
      return false;
    }

    final String message = ((YTDistributedLockException) obj).getMessage();
    return getMessage() != null && getMessage().equals(message);
  }
}
