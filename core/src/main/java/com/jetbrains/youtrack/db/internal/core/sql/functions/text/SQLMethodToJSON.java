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
package com.jetbrains.youtrack.db.internal.core.sql.functions.text;

import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.AbstractSQLMethod;
import java.util.Map;

/**
 * Converts a document in JSON string.
 */
public class SQLMethodToJSON extends AbstractSQLMethod {

  public static final String NAME = "tojson";

  public SQLMethodToJSON() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "toJSON([<format>])";
  }

  @Override
  public Object execute(
      Object iThis,
      Identifiable iCurrentRecord,
      CommandContext iContext,
      Object ioResult,
      Object[] iParams) {
    if (iThis == null) {
      return null;
    }

    final String format = iParams.length > 0 ? ((String) iParams[0]).replace("\"", "") : null;

    if (iThis instanceof Result) {
      iThis = ((Result) iThis).toEntity();
    }
    if (iThis instanceof Record record) {

      if (record.isUnloaded()) {
        record = iContext.getDatabase().bindToSession(record);
      }

      return iParams.length == 1 ? record.toJSON(format) : record.toJSON();
    } else if (iThis instanceof Map) {

      final EntityImpl entity = new EntityImpl();
      //noinspection unchecked
      entity.fromMap((Map<String, Object>) iThis);
      return iParams.length == 1 ? entity.toJSON(format) : entity.toJSON();
    } else if (MultiValue.isMultiValue(iThis)) {
      StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      for (Object o : MultiValue.getMultiValueIterable(iThis)) {
        if (!first) {
          builder.append(",");
        }
        builder.append(execute(o, iCurrentRecord, iContext, ioResult, iParams));
        first = false;
      }

      builder.append("]");
      return builder.toString();
    }
    return null;
  }
}
