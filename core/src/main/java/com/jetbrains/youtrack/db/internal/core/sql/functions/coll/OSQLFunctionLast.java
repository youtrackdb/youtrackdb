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
package com.jetbrains.youtrack.db.internal.core.sql.functions.coll;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterItem;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionConfigurableAbstract;

/**
 * Extract the last item of multi values (arrays, collections and maps) or return the same value for
 * non multi-value types.
 */
public class OSQLFunctionLast extends OSQLFunctionConfigurableAbstract {

  public static final String NAME = "last";

  public OSQLFunctionLast() {
    super(NAME, 1, 1);
  }

  public Object execute(
      Object iThis,
      final YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      final CommandContext iContext) {
    Object value = iParams[0];

    if (value instanceof OSQLFilterItem) {
      value = ((OSQLFilterItem) value).getValue(iCurrentRecord, iCurrentResult, iContext);
    }

    if (OMultiValue.isMultiValue(value)) {
      value = OMultiValue.getLastValue(value);
    }

    return value;
  }

  public String getSyntax(YTDatabaseSession session) {
    return "last(<field>)";
  }
}
