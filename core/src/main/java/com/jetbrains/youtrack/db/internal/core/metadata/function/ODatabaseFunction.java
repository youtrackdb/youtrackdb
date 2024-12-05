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
package com.jetbrains.youtrack.db.internal.core.metadata.function;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunction;
import java.util.List;

/**
 * Dynamic function factory bound to the database's functions
 */
public class ODatabaseFunction implements OSQLFunction {

  private final OFunction f;

  public ODatabaseFunction(final OFunction f) {
    this.f = f;
  }

  @Override
  public Object execute(
      Object iThis,
      final YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iFuncParams,
      final CommandContext iContext) {
    return f.executeInContext(iContext, iFuncParams);
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  public boolean filterResult() {
    return false;
  }

  @Override
  public String getName(YTDatabaseSession session) {
    return f.getName(session);
  }

  @Override
  public int getMinParams() {
    return 0;
  }

  @Override
  public int getMaxParams(YTDatabaseSession session) {
    return f.getParameters(session) != null ? f.getParameters(session).size() : 0;
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    final StringBuilder buffer = new StringBuilder(512);
    buffer.append(f.getName(session));
    buffer.append('(');
    final List<String> params = f.getParameters(session);
    for (int p = 0; p < params.size(); ++p) {
      if (p > 0) {
        buffer.append(',');
      }
      buffer.append(params.get(p));
    }
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public void setResult(final Object iResult) {
  }

  @Override
  public void config(final Object[] configuredParameters) {
  }

  @Override
  public boolean shouldMergeDistributedResult() {
    return false;
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    return null;
  }
}
