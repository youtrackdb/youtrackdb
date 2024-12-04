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
package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.YTHighLevelException;
import com.orientechnologies.orient.core.id.YTRID;
import java.io.Serial;
import java.util.Objects;

public class YTRecordNotFoundException extends YTCoreException implements YTHighLevelException {

  @Serial
  private static final long serialVersionUID = -265573123216968L;

  private final YTRID rid;

  public YTRecordNotFoundException(final YTRecordNotFoundException exception) {
    super(exception);
    this.rid = exception.rid;
  }

  public YTRecordNotFoundException(final YTRID iRID) {
    super("The record with id '" + iRID + "' was not found");
    rid = iRID;
  }

  public YTRecordNotFoundException(final YTRID iRID, final String message) {
    super(message);
    rid = iRID;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof YTRecordNotFoundException)) {
      return false;
    }

    if (rid == null && ((YTRecordNotFoundException) obj).rid == null) {
      return toString().equals(obj.toString());
    }

    return rid != null && rid.equals(((YTRecordNotFoundException) obj).rid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rid);
  }

  public YTRID getRid() {
    return rid;
  }
}
