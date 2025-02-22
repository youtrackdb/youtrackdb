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
package com.jetbrains.youtrack.db.internal.core.sql.functions.math;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import java.util.Collection;
import java.util.List;

/**
 * Compute the maximum value for a field. Uses the context to save the last maximum number. When
 * different Number class are used, take the class with most precision.
 */
public class SQLFunctionMax extends SQLFunctionMathAbstract {

  public static final String NAME = "max";

  private Object context;

  public SQLFunctionMax() {
    super(NAME, 1, -1);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public Object execute(
      Object iThis,
      final Identifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      CommandContext iContext) {

    // calculate max value for current record
    // consider both collection of parameters and collection in each parameter
    Object max = null;
    for (Object item : iParams) {
      if (item instanceof Collection<?>) {
        for (Object subitem : ((Collection<?>) item)) {
          if (max == null || subitem != null && ((Comparable) subitem).compareTo(max) > 0) {
            max = subitem;
          }
        }
      } else {
        if ((item instanceof Number) && (max instanceof Number)) {
          Number[] converted = PropertyType.castComparableNumber((Number) item, (Number) max);
          item = converted[0];
          max = converted[1];
        }
        if (max == null || item != null && ((Comparable) item).compareTo(max) > 0) {
          max = item;
        }
      }
    }

    // what to do with the result, for current record, depends on how this function has been invoked
    // for an unique result aggregated from all output records
    if (aggregateResults() && max != null) {
      if (context == null)
      // FIRST TIME
      {
        context = max;
      } else {
        if (context instanceof Number && max instanceof Number) {
          final Number[] casted = PropertyType.castComparableNumber((Number) context, (Number) max);
          context = casted[0];
          max = casted[1];
        }
        if (((Comparable<Object>) context).compareTo(max) < 0)
        // BIGGER
        {
          context = max;
        }
      }

      return null;
    }

    // for non aggregated results (a result per output record)
    return max;
  }

  public boolean aggregateResults() {
    // LET definitions (contain $current) does not require results aggregation
    return ((configuredParameters.length == 1)
        && !configuredParameters[0].toString().contains("$current"));
  }

  public String getSyntax(DatabaseSession session) {
    return "max(<field> [,<field>*])";
  }

  @Override
  public Object getResult() {
    return context;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    Comparable<Object> context = null;
    for (Object iParameter : resultsToMerge) {
      final Comparable<Object> value = (Comparable<Object>) iParameter;

      if (context == null)
      // FIRST TIME
      {
        context = value;
      } else if (context.compareTo(value) < 0)
      // BIGGER
      {
        context = value;
      }
    }
    return context;
  }
}
