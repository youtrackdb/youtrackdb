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
package com.jetbrains.youtrack.db.internal.core.sql.functions.stat;

import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.sql.functions.SQLFunctionAbstract;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Computes the percentile for a field. Nulls are ignored in the calculation.
 */
public class SQLFunctionPercentile extends SQLFunctionAbstract {

  public static final String NAME = "percentile";

  protected List<Double> quantiles = new ArrayList<Double>();
  private final List<Number> values = new ArrayList<Number>();

  public SQLFunctionPercentile() {
    this(NAME, 2, -1);
  }

  public SQLFunctionPercentile(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMaxParams, iMaxParams);
  }

  @Override
  public Object execute(
      Object iThis,
      Identifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      CommandContext iContext) {

    if (quantiles.isEmpty()) { // set quantiles once
      for (int i = 1; i < iParams.length; ++i) {
        this.quantiles.add(Double.parseDouble(iParams[i].toString()));
      }
    }

    if (iParams[0] instanceof Number) {
      addValue((Number) iParams[0]);
    } else if (MultiValue.isMultiValue(iParams[0])) {
      for (Object n : MultiValue.getMultiValueIterable(iParams[0])) {
        addValue((Number) n);
      }
    }
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    if (returnDistributedResult()) {
      return values;
    } else {
      return this.evaluate(this.values);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    if (returnDistributedResult()) {
      List<Number> dValues = new ArrayList<Number>();
      for (Object iParameter : resultsToMerge) {
        dValues.addAll((List<Number>) iParameter);
      }
      return this.evaluate(dValues);
    }

    if (!resultsToMerge.isEmpty()) {
      return resultsToMerge.get(0);
    }

    return null;
  }

  @Override
  public String getSyntax(DatabaseSession session) {
    return NAME + "(<field>, <quantile> [,<quantile>*])";
  }

  private void addValue(Number value) {
    if (value != null) {
      this.values.add(value);
    }
  }

  private Object evaluate(List<Number> iValues) {
    if (iValues.isEmpty()) { // result set is empty
      return null;
    }
    if (quantiles.size() > 1) {
      List<Number> results = new ArrayList<Number>();
      for (Double q : this.quantiles) {
        results.add(this.evaluate(iValues, q));
      }
      return results;
    } else {
      return this.evaluate(iValues, this.quantiles.get(0));
    }
  }

  private Number evaluate(List<Number> iValues, double iQuantile) {
    Collections.sort(
        iValues,
        new Comparator<Number>() {
          @Override
          public int compare(Number o1, Number o2) {
            Double d1 = o1.doubleValue();
            Double d2 = o2.doubleValue();
            return d1.compareTo(d2);
          }
        });

    double n = iValues.size();
    double pos = iQuantile * (n + 1);

    if (pos < 1) {
      return iValues.get(0);
    }
    if (pos >= n) {
      return iValues.get((int) n - 1);
    }

    double fpos = Math.floor(pos);
    int intPos = (int) fpos;
    double dif = pos - fpos;

    double lower = iValues.get(intPos - 1).doubleValue();
    double upper = iValues.get(intPos).doubleValue();
    return lower + dif * (upper - lower);
  }
}
