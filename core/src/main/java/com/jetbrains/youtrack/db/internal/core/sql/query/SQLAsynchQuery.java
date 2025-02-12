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
package com.jetbrains.youtrack.db.internal.core.sql.query;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestAsynch;
import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.List;
import java.util.Map;

/**
 * SQL asynchronous query. When executed the caller does not wait for the execution, rather the
 * listener will be called for each item found in the query. SQLAsynchQuery has been built on top of
 * this. NOTE: if you're working with remote databases don't execute any remote call inside the
 * callback function because the network channel is locked until the query command has finished.
 *
 * @param <T>
 * @see SQLSynchQuery
 */
public class SQLAsynchQuery<T extends Object> extends SQLQuery<T>
    implements CommandRequestAsynch {

  private static final long serialVersionUID = 1L;

  /**
   * Empty constructor for unmarshalling.
   */
  public SQLAsynchQuery() {
  }

  public SQLAsynchQuery(final String iText) {
    this(iText, null);
  }

  public SQLAsynchQuery(final String iText, final CommandResultListener iResultListener) {
    this(iText, -1, iResultListener);
  }

  public SQLAsynchQuery(
      final String iText,
      final int iLimit,
      final String iFetchPlan,
      final Map<Object, Object> iArgs,
      final CommandResultListener iResultListener) {
    this(iText, iLimit, iResultListener);
    this.fetchPlan = iFetchPlan;
    this.parameters = iArgs;
  }

  public SQLAsynchQuery(
      final String iText, final int iLimit, final CommandResultListener iResultListener) {
    super(iText);
    limit = iLimit;
    resultListener = iResultListener;
  }

  @Override
  public List<T> run(DatabaseSessionInternal session, Object... iArgs) {
    if (resultListener == null) {
      throw new CommandExecutionException(session, "Listener not found on asynch query");
    }

    return super.run(session, iArgs);
  }

  /**
   * Sets default non idempotent to avoid custom query deadlocks database.
   */
  @Override
  public boolean isIdempotent() {
    return true;
  }

  @Override
  public boolean isAsynchronous() {
    return true;
  }
}
