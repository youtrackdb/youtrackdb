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
package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.common.util.OCallable;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.OCommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTSecurityException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORestrictedAccessHook;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORestrictedOperation;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityInternal;
import com.jetbrains.youtrack.db.internal.core.query.live.OLiveQueryHook;
import com.jetbrains.youtrack.db.internal.core.query.live.OLiveQueryListener;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.OLegacyResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.query.OLiveResultListener;
import java.util.Map;
import java.util.Random;

/**
 *
 */
public class OCommandExecutorSQLLiveSelect extends OCommandExecutorSQLSelect
    implements OLiveQueryListener {

  public static final String KEYWORD_LIVE_SELECT = "LIVE SELECT";
  private YTDatabaseSessionInternal execDb;
  private int token;
  private static final Random random = new Random();

  public OCommandExecutorSQLLiveSelect() {
  }

  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    try {
      final YTDatabaseSessionInternal db = getDatabase();
      execInSeparateDatabase(
          new OCallable() {
            @Override
            public Object call(Object iArgument) {
              return execDb = db.copy();
            }
          });

      synchronized (random) {
        token = random.nextInt(); // TODO do something better ;-)!
      }
      subscribeToLiveQuery(token, db);
      bindDefaultContextVariables();

      if (iArgs != null)
      // BIND ARGUMENTS INTO CONTEXT TO ACCESS FROM ANY POINT (EVEN FUNCTIONS)
      {
        for (Map.Entry<Object, Object> arg : iArgs.entrySet()) {
          context.setVariable(arg.getKey().toString(), arg.getValue());
        }
      }

      if (timeoutMs > 0) {
        getContext().beginExecution(timeoutMs, timeoutStrategy);
      }

      EntityImpl result = new EntityImpl();
      result.field("token", token); // TODO change this name...?

      ((OLegacyResultSet) getResult(querySession)).add(result);
      return getResult(querySession);
    } finally {
      if (request != null && request.getResultListener() != null) {
        request.getResultListener().end();
      }
    }
  }

  private void subscribeToLiveQuery(Integer token, YTDatabaseSessionInternal db) {
    OLiveQueryHook.subscribe(token, this, db);
  }

  public void onLiveResult(final ORecordOperation iOp) {

    YTDatabaseSessionInternal oldThreadLocal = ODatabaseRecordThreadLocal.instance().getIfDefined();
    execDb.activateOnCurrentThread();

    try {
      final YTIdentifiable value = iOp.record;

      if (!matchesTarget(value)) {
        return;
      }
      if (!matchesFilters(value)) {
        return;
      }
      if (!checkSecurity(value)) {
        return;
      }
    } finally {
      if (oldThreadLocal == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(oldThreadLocal);
      }
    }
    final OCommandResultListener listener = request.getResultListener();
    if (listener instanceof OLiveResultListener) {
      execInSeparateDatabase(
          new OCallable() {
            @Override
            public Object call(Object iArgument) {
              execDb.activateOnCurrentThread();
              ((OLiveResultListener) listener).onLiveResult(token, iOp);
              return null;
            }
          });
    }
  }

  protected void execInSeparateDatabase(final OCallable iCallback) {
    final YTDatabaseSessionInternal prevDb = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      iCallback.call(null);
    } finally {
      if (prevDb != null) {
        ODatabaseRecordThreadLocal.instance().set(prevDb);
      } else {
        ODatabaseRecordThreadLocal.instance().remove();
      }
    }
  }

  private boolean checkSecurity(YTIdentifiable value) {
    try {
      // TODO check this!
      execDb.checkSecurity(
          ORule.ResourceGeneric.CLASS,
          ORole.PERMISSION_READ,
          ((EntityImpl) value.getRecord()).getClassName());
    } catch (YTSecurityException ignore) {
      return false;
    }
    OSecurityInternal security = execDb.getSharedContext().getSecurity();
    boolean allowedByPolicy = security.canRead(execDb, value.getRecord());
    return allowedByPolicy
        && ORestrictedAccessHook.isAllowed(
        execDb, value.getRecord(), ORestrictedOperation.ALLOW_READ, false);
  }

  private boolean matchesFilters(YTIdentifiable value) {
    if (this.compiledFilter == null || this.compiledFilter.getRootCondition() == null) {
      return true;
    }
    if (!(value instanceof EntityImpl)) {
      value = value.getRecord();
    }
    return !(Boolean.FALSE.equals(
        compiledFilter.evaluate(value, (EntityImpl) value, getContext())));
  }

  private boolean matchesTarget(YTIdentifiable value) {
    if (!(value instanceof EntityImpl)) {
      return false;
    }
    final String className = ((EntityImpl) value).getClassName();
    if (className == null) {
      return false;
    }
    final YTClass docClass = execDb.getMetadata().getSchema().getClass(className);
    if (docClass == null) {
      return false;
    }

    if (this.parsedTarget.getTargetClasses() != null) {
      for (String clazz : parsedTarget.getTargetClasses().keySet()) {
        if (docClass.isSubClassOf(clazz)) {
          return true;
        }
      }
    }
    if (this.parsedTarget.getTargetRecords() != null) {
      for (YTIdentifiable r : parsedTarget.getTargetRecords()) {
        if (r.getIdentity().equals(value.getIdentity())) {
          return true;
        }
      }
    }
    if (this.parsedTarget.getTargetClusters() != null) {
      final String clusterName = execDb.getClusterNameById(value.getIdentity().getClusterId());
      if (clusterName != null) {
        for (String cluster : parsedTarget.getTargetClusters().keySet()) {
          if (clusterName.equalsIgnoreCase(cluster)) { // make it case insensitive in 3.0?
            return true;
          }
        }
      }
    }
    return false;
  }

  public void onLiveResultEnd() {
    if (request.getResultListener() instanceof OLiveResultListener) {
      ((OLiveResultListener) request.getResultListener()).onUnsubscribe(token);
    }

    if (execDb != null) {
      YTDatabaseSessionInternal oldThreadDB = ODatabaseRecordThreadLocal.instance().getIfDefined();
      execDb.activateOnCurrentThread();
      execDb.close();
      if (oldThreadDB == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(oldThreadDB);
      }
    }
  }

  @Override
  public OCommandExecutorSQLSelect parse(final OCommandRequest iRequest) {
    final OCommandRequestText requestText = (OCommandRequestText) iRequest;
    final String originalText = requestText.getText();
    final String remainingText = requestText.getText().trim().substring(5).trim();
    requestText.setText(remainingText);
    try {
      return super.parse(iRequest);
    } finally {
      requestText.setText(originalText);
    }
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
  }
}
