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

import com.orientechnologies.orient.core.db.YTDatabaseSession;

/**
 * Computes the median for a field. Nulls are ignored in the calculation.
 *
 * <p>Extends and forces the {@link OSQLFunctionPercentile} with the 50th percentile.
 */
public class OSQLFunctionMedian extends OSQLFunctionPercentile {

  public static final String NAME = "median";

  public OSQLFunctionMedian() {
    super(NAME, 1, 1);
    this.quantiles.add(.5);
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return NAME + "(<field>)";
  }
}
