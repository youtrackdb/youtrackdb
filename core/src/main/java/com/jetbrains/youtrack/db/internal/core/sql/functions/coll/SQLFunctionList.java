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

import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * This operator add an item in a list. The list accepts duplicates.
 */
public class SQLFunctionList extends SQLFunctionMultiValueAbstract<List<Object>> {

  public static final String NAME = "list";

  public SQLFunctionList() {
    super(NAME, 1, -1);
  }

  public Object execute(
      Object iThis,
      final Identifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iParams,
      CommandContext iContext) {
    if (iParams.length > 1)
    // IN LINE MODE
    {
      context = new ArrayList<Object>();
    }

    for (Object value : iParams) {
      if (value != null) {
        if (iParams.length == 1 && context == null)
        // AGGREGATION MODE (STATEFULL)
        {
          context = new ArrayList<Object>();
        }

        if (value instanceof Map) {
          context.add(value);
        } else {
          MultiValue.add(context, value);
        }
      }
    }
    return prepareResult(context);
  }

  public String getSyntax(DatabaseSession session) {
    return "list(<value>*)";
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  @Override
  public List<Object> getResult() {
    final List<Object> res = context;
    context = null;
    return prepareResult(res);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    if (returnDistributedResult()) {
      final Collection<Object> result = new HashSet<Object>();
      for (Object iParameter : resultsToMerge) {
        final Map<String, Object> container =
            (Map<String, Object>) ((Collection<?>) iParameter).iterator().next();
        result.addAll((Collection<?>) container.get("context"));
      }
      return result;
    }

    if (!resultsToMerge.isEmpty()) {
      return resultsToMerge.get(0);
    }

    return null;
  }

  protected List<Object> prepareResult(List<Object> res) {
    if (returnDistributedResult()) {
      final Map<String, Object> entity = new HashMap<String, Object>();
      entity.put("node", getDistributedStorageId());
      entity.put("context", res);
      return Collections.singletonList(entity);
    } else {
      return res;
    }
  }
}
