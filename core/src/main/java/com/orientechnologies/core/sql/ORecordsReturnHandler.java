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

package com.orientechnologies.core.sql;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class ORecordsReturnHandler implements OReturnHandler {

  private final Object returnExpression;
  private final OCommandContext context;
  private List<Object> results;

  protected ORecordsReturnHandler(final Object returnExpression, final OCommandContext context) {
    this.returnExpression = returnExpression;
    this.context = context;
  }

  @Override
  public void reset() {
    results = new ArrayList<Object>();
  }

  @Override
  public Object ret() {
    return results;
  }

  protected void storeResult(final YTEntityImpl result) {
    final YTEntityImpl processedResult = preprocess(result);

    results.add(evaluateExpression(processedResult));
  }

  protected abstract YTEntityImpl preprocess(final YTEntityImpl result);

  private Object evaluateExpression(final YTEntityImpl record) {
    if (returnExpression == null) {
      return record;
    } else {
      final Object itemResult;
      final YTEntityImpl wrappingDoc;
      context.setVariable("current", record);

      itemResult =
          OSQLHelper.getValue(returnExpression, ((YTIdentifiable) record).getRecord(), context);
      if (itemResult instanceof YTIdentifiable) {
        return itemResult;
      }

      // WRAP WITH ODOCUMENT TO BE TRANSFERRED THROUGH BINARY DRIVER
      return new YTEntityImpl("value", itemResult);
    }
  }
}
