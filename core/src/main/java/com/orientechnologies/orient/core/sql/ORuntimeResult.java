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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Handles runtime results.
 */
public class ORuntimeResult {

  private final Object fieldValue;
  private final Map<String, Object> projections;
  private final OResultInternal value;
  private final OCommandContext context;

  public ORuntimeResult(
      final Object iFieldValue,
      final Map<String, Object> iProjections,
      final int iProgressive,
      final OCommandContext iContext) {
    fieldValue = iFieldValue;
    projections = iProjections;
    context = iContext;
    value = new OResultInternal(iContext.getDatabase());
  }


  private static boolean entriesPersistent(Collection<YTIdentifiable> projectionValue) {
    for (YTIdentifiable rec : projectionValue) {
      if (rec != null && !rec.getIdentity().isPersistent()) {
        return false;
      }
    }

    return true;
  }

  public static OResultInternal getResult(
      YTDatabaseSessionInternal session, final OResultInternal iValue,
      final Map<String, Object> iProjections) {
    if (iValue != null) {
      boolean canExcludeResult = false;

      for (Entry<String, Object> projection : iProjections.entrySet()) {
        if (!iValue.hasProperty(projection.getKey())) {
          // ONLY IF NOT ALREADY CONTAINS A VALUE, OTHERWISE HAS BEEN SET MANUALLY (INDEX?)
          final Object v = projection.getValue();
          if (v instanceof OSQLFunctionRuntime f) {
            canExcludeResult = f.filterResult();
            Object fieldValue = f.getResult(session);
            if (fieldValue != null) {
              iValue.setProperty(projection.getKey(), fieldValue);
            }
          }
        }
      }

      if (canExcludeResult && iValue.getPropertyNames().isEmpty()) {
        // RESULT EXCLUDED FOR EMPTY RECORD
        return null;
      }
    }

    return iValue;
  }

  /**
   * Set a single value. This is useful in case of query optimization like with indexes
   *
   * @param iName  Field name
   * @param iValue Field value
   */
  public void applyValue(final String iName, final Object iValue) {
    value.setProperty(iName, iValue);
  }

  public OResultInternal getResult(YTDatabaseSessionInternal session) {
    return getResult(session, value, projections);
  }

  public Object getFieldValue() {
    return fieldValue;
  }
}
