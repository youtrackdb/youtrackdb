/*
 *
 *  *  Copyright YouTrackDB
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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.query.LiveQueryMonitor;
import com.jetbrains.youtrack.db.api.query.LiveQueryResultListener;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import javax.annotation.Nonnull;

/**
 * SQL live query. <br>
 * <br>
 * The statement syntax is the same as a normal SQL SELECT statement, but with LIVE as prefix: <br>
 * <br>
 * LIVE SELECT FROM Foo WHERE name = 'bar' <br>
 * <br>
 * Executing this query, the caller will subscribe to receive changes happening in the database,
 * that match this query condition. The query returns a query token in the result set. To
 * unsubscribe, the user has to execute another live query with the following syntax: <br>
 * <br>
 * LIVE UNSUBSCRIBE &lt;token&gt; <br>
 * <br>
 * The callback passed as second parameter will be invoked every time a record is
 * created/updated/deleted and it matches the query conditions.
 */
public class LiveQuery<T> extends SQLSynchQuery<T> {

  public LiveQuery() {
  }

  public LiveQuery(String iText, final LiveResultListener iResultListener) {
    super(iText);
    setResultListener(new LocalLiveResultListener(iResultListener));
  }

  @Override
  public <RET> RET execute(@Nonnull DatabaseSessionInternal db, Object... iArgs) {
    if (db.isRemote()) {
      BackwardLiveQueryResultListener listener = new BackwardLiveQueryResultListener();
      LiveQueryMonitor monitor = db.live(getText(), listener, iArgs);
      listener.token = monitor.getMonitorId();
      EntityImpl entity = new EntityImpl(null);
      entity.setProperty("token", listener.token);
      LegacyResultSet<EntityImpl> result = new BasicLegacyResultSet<>();
      result.add(entity);
      return (RET) result;
    }
    return super.execute(db, iArgs);
  }

  private class BackwardLiveQueryResultListener implements LiveQueryResultListener {

    protected int token;

    @Override
    public void onCreate(DatabaseSessionInternal database, Result data) {
      ((LocalLiveResultListener) getResultListener())
          .onLiveResult(database,
              token,
              new RecordOperation((RecordAbstract) data.asEntity(), RecordOperation.CREATED));
    }

    @Override
    public void onUpdate(DatabaseSessionInternal database, Result before, Result after) {
      ((LocalLiveResultListener) getResultListener())
          .onLiveResult(database,
              token,
              new RecordOperation((RecordAbstract) after.asEntity(), RecordOperation.UPDATED));
    }

    @Override
    public void onDelete(DatabaseSessionInternal database, Result data) {
      ((LocalLiveResultListener) getResultListener())
          .onLiveResult(database,
              token,
              new RecordOperation((RecordAbstract) data.asEntity(), RecordOperation.DELETED));
    }

    @Override
    public void onError(DatabaseSession database, BaseException exception) {
      ((LocalLiveResultListener) getResultListener()).onError(token);
    }

    @Override
    public void onEnd(DatabaseSession database) {
      ((LocalLiveResultListener) getResultListener()).onUnsubscribe(token);
    }
  }
}
