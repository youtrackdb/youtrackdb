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

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OAbstractSQLMethod;
import com.jetbrains.youtrack.db.internal.core.util.ODateHelper;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Transforms a value to date. If the conversion is not possible, null is returned.
 */
public class OSQLMethodAsDate extends OAbstractSQLMethod {

  public static final String NAME = "asdate";

  public OSQLMethodAsDate() {
    super(NAME, 0, 0);
  }

  @Override
  public String getSyntax() {
    return "asDate()";
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      CommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (iThis != null) {
      if (iThis instanceof Date) {
        Calendar cal = new GregorianCalendar();
        cal.setTime((Date) iThis);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
      } else if (iThis instanceof Number) {
        Date val = new Date(((Number) iThis).longValue());
        Calendar cal = new GregorianCalendar();
        cal.setTime(val);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
      } else {
        try {
          return ODateHelper.getDateFormatInstance(ODatabaseRecordThreadLocal.instance().get())
              .parse(iThis.toString());
        } catch (ParseException e) {
          LogManager.instance().error(this, "Error during %s execution", e, NAME);
        }
      }
    }
    return null;
  }
}
