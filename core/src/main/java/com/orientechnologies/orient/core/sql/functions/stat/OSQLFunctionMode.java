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
package com.orientechnologies.orient.core.sql.functions.stat;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Compute the mode (or multimodal) value for a field. The scores in the field's distribution that
 * occurs more frequently. Nulls are ignored in the calculation.
 */
public class OSQLFunctionMode extends OSQLFunctionAbstract {

  public static final String NAME = "mode";

  private final Object2IntOpenHashMap<Object> seen = new Object2IntOpenHashMap<>();
  private int max = 0;
  private final List<Object> maxElems = new ArrayList<Object>();

  public OSQLFunctionMode() {
    super(NAME, 1, 1);
    seen.defaultReturnValue(-1);
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      OCommandContext iContext) {

    if (OMultiValue.isMultiValue(iParams[0])) {
      for (Object o : OMultiValue.getMultiValueIterable(iParams[0])) {
        max = evaluate(o, 1, seen, maxElems, max);
      }
    } else {
      max = evaluate(iParams[0], 1, seen, maxElems, max);
    }
    return getResult();
  }

  @Override
  public Object getResult() {
    if (returnDistributedResult()) {
      return seen;
    } else {
      return maxElems.isEmpty() ? null : maxElems;
    }
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return NAME + "(<field>)";
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    if (returnDistributedResult()) {
      Object2IntOpenHashMap<Object> dSeen = new Object2IntOpenHashMap<>();
      dSeen.defaultReturnValue(-1);

      int dMax = 0;
      List<Object> dMaxElems = new ArrayList<Object>();
      for (Object iParameter : resultsToMerge) {
        final Map<Object, Integer> mSeen = (Map<Object, Integer>) iParameter;
        for (Entry<Object, Integer> o : mSeen.entrySet()) {
          dMax = this.evaluate(o.getKey(), o.getValue(), dSeen, dMaxElems, dMax);
        }
      }
      return dMaxElems;
    }

    if (!resultsToMerge.isEmpty()) {
      return resultsToMerge.get(0);
    }

    return null;
  }

  private int evaluate(
      Object value,
      int times,
      Object2IntOpenHashMap<Object> iSeen,
      List<Object> iMaxElems,
      int iMax) {
    if (value != null) {
      if (iSeen.containsKey(value)) {
        iSeen.put(value, iSeen.getInt(value) + times);
      } else {
        iSeen.put(value, times);
      }
      if (iSeen.getInt(value) > iMax) {
        iMax = iSeen.getInt(value);
        iMaxElems.clear();
        iMaxElems.add(value);
      } else if (iSeen.getInt(value) == iMax) {
        iMaxElems.add(value);
      }
    }
    return iMax;
  }
}
