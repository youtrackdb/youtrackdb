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
package com.orientechnologies.core.exception;

import com.orientechnologies.common.concur.YTNeedRetryException;
import com.orientechnologies.common.exception.YTHighLevelException;
import com.orientechnologies.core.id.YTRID;
import java.util.Objects;

/**
 * Exception thrown when a create operation get a non expected RID. This could happen with
 * distributed inserts. The client should retry to re-execute the operation.
 */
public class YTConcurrentCreateException extends YTNeedRetryException implements
    YTHighLevelException {

  private static final long serialVersionUID = 1L;

  private YTRID expectedRid;
  private YTRID actualRid;

  public YTConcurrentCreateException(YTConcurrentCreateException exception) {
    super(exception, null);

    this.expectedRid = exception.expectedRid;
    this.actualRid = exception.actualRid;
  }

  protected YTConcurrentCreateException(final String message) {
    super(message);
  }

  public YTConcurrentCreateException(final YTRID expectedRID, final YTRID actualRid) {
    super(makeMessage(expectedRID, actualRid));

    this.expectedRid = expectedRID;
    this.actualRid = actualRid;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof YTConcurrentCreateException other)) {
      return false;
    }

    return expectedRid.equals(other.expectedRid) && actualRid.equals(other.actualRid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expectedRid, actualRid);
  }

  public YTRID getExpectedRid() {
    return expectedRid;
  }

  public YTRID getActualRid() {
    return actualRid;
  }

  private static String makeMessage(YTRID expectedRid, YTRID actualRid) {
    String sb =
        "Cannot create the record "
            + expectedRid
            + " because the assigned RID was "
            + actualRid
            + " instead";
    return sb;
  }
}
