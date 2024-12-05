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
package com.jetbrains.youtrack.db.internal.common.concur;

import com.jetbrains.youtrack.db.internal.common.exception.OErrorCode;
import com.jetbrains.youtrack.db.internal.core.exception.YTCoreException;

/**
 * Abstract base exception to extend for all the exception that report to the user it has been
 * thrown but re-executing it could succeed.
 */
public abstract class YTNeedRetryException extends YTCoreException {

  private static final long serialVersionUID = 1L;

  protected YTNeedRetryException(final YTNeedRetryException exception) {
    super(exception, null);
  }

  protected YTNeedRetryException(final YTNeedRetryException exception, OErrorCode errorCode) {
    super(exception, errorCode);
  }

  protected YTNeedRetryException(final String message, OErrorCode errorCode) {
    super(message, null, errorCode);
  }

  protected YTNeedRetryException(final String message) {
    super(message);
  }
}
