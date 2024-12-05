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

package com.jetbrains.youtrack.db.internal.common.exception;

public abstract class YTException extends RuntimeException {

  private static final long serialVersionUID = 3882447822497861424L;

  public static YTException wrapException(final YTException exception, final Throwable cause) {
    if (cause instanceof YTHighLevelException) {
      return (YTException) cause;
    }

    exception.initCause(cause);
    return exception;
  }

  public YTException(final String message) {
    super(message);
  }

  /**
   * This constructor is needed to restore and reproduce exception on client side in case of remote
   * storage exception handling. Please create "copy constructor" for each exception which has
   * current one as a parent.
   */
  public YTException(final YTException exception) {
    super(exception.getMessage(), exception.getCause());
  }
}
