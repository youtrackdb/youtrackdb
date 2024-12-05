/*
 *
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.core.sql.functions.conversion;

import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OAbstractSQLMethod;
import com.jetbrains.youtrack.db.internal.core.util.ODateHelper;
import java.text.ParseException;
import java.util.Date;

/**
 * Transforms a value to datetime. If the conversion is not possible, null is returned.
 */
public class OSQLMethodAsDateTime extends OAbstractSQLMethod {

  public static final String NAME = "asdatetime";

  public OSQLMethodAsDateTime() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "asDatetime()";
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (iThis != null) {
      if (iThis instanceof Date) {
        return iThis;
      } else if (iThis instanceof Number) {
        return new Date(((Number) iThis).longValue());
      } else {
        try {
          return ODateHelper.getDateTimeFormatInstance(ODatabaseRecordThreadLocal.instance().get())
              .parse(iThis.toString());
        } catch (ParseException e) {
          OLogManager.instance().error(this, "Error during %s execution", e, NAME);
          // IGNORE IT: RETURN NULL
        }
      }
    }
    return null;
  }
}
