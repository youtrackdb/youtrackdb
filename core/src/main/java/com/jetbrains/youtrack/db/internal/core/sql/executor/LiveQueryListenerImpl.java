package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.util.OCallable;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YTLiveQueryBatchResultListener;
import com.jetbrains.youtrack.db.internal.core.db.YTLiveQueryResultListener;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.query.live.OLiveQueryHookV2;
import com.jetbrains.youtrack.db.internal.core.query.live.OLiveQueryListenerV2;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLWhereClause;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 *
 */
public class LiveQueryListenerImpl implements OLiveQueryListenerV2 {

  public static final String BEFORE_METADATA_KEY = "$$before$$";
  private final YTLiveQueryResultListener clientListener;
  private YTDatabaseSessionInternal execDb;

  private final SQLSelectStatement statement;
  private String className;
  private List<YTRecordId> rids;

  private final Map<Object, Object> params;

  private final int token;
  private static final Random random = new Random();

  public LiveQueryListenerImpl(
      YTLiveQueryResultListener clientListener, String query, YTDatabaseSessionInternal db,
      Object[] iArgs) {
    this(clientListener, query, db, toPositionalParams(iArgs));
  }

  public LiveQueryListenerImpl(
      YTLiveQueryResultListener clientListener,
      String query,
      YTDatabaseSessionInternal db,
      Map<Object, Object> iArgs) {
    this.clientListener = clientListener;
    this.params = iArgs;

    if (query.trim().toLowerCase().startsWith("live ")) {
      query = query.trim().substring(5);
    }
    SQLStatement stm = OSQLEngine.parse(query, db);
    if (!(stm instanceof SQLSelectStatement)) {
      throw new YTCommandExecutionException(
          "Only SELECT statement can be used as a live query: " + query);
    }
    this.statement = (SQLSelectStatement) stm;
    validateStatement(statement, db);
    if (statement.getTarget().getItem().getIdentifier() != null) {
      this.className = statement.getTarget().getItem().getIdentifier().getStringValue();
      if (!db
          .getMetadata()
          .getImmutableSchemaSnapshot()
          .existsClass(className)) {
        throw new YTCommandExecutionException(
            "Class " + className + " not found in the schema: " + query);
      }
    } else if (statement.getTarget().getItem().getRids() != null) {
      var context = new BasicCommandContext();
      context.setDatabase(db);
      this.rids =
          statement.getTarget().getItem().getRids().stream()
              .map(x -> x.toRecordId(new YTResultInternal(db), context))
              .collect(Collectors.toList());
    }
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
    OLiveQueryHookV2.subscribe(token, this, db);

    CommandContext ctx = new BasicCommandContext();
    if (iArgs != null)
    // BIND ARGUMENTS INTO CONTEXT TO ACCESS FROM ANY POINT (EVEN FUNCTIONS)
    {
      for (Map.Entry<Object, Object> arg : iArgs.entrySet()) {
        ctx.setVariable(arg.getKey().toString(), arg.getValue());
      }
    }
  }

  private void validateStatement(SQLSelectStatement statement, YTDatabaseSessionInternal db) {
    if (statement.getProjection() != null) {
      if (statement.getProjection().getItems().stream().anyMatch(x -> x.isAggregate(db))) {
        throw new YTCommandExecutionException(
            "Aggregate Projections cannot be used in live query " + statement);
      }
    }
    if (statement.getTarget().getItem().getIdentifier() == null
        && statement.getTarget().getItem().getRids() == null) {
      throw new YTCommandExecutionException(
          "Live queries can only be executed against a Class or on RIDs" + statement);
    }
    if (statement.getOrderBy() != null) {
      throw new YTCommandExecutionException("Live queries do not support ORDER BY " + statement);
    }
    if (statement.getGroupBy() != null) {
      throw new YTCommandExecutionException("Live queries do not support GROUP BY " + statement);
    }
    if (statement.getSkip() != null || statement.getLimit() != null) {
      throw new YTCommandExecutionException("Live queries do not support SKIP/LIMIT " + statement);
    }
  }

  public int getToken() {
    return token;
  }

  @Override
  public void onLiveResults(List<OLiveQueryHookV2.OLiveQueryOp> iRecords) {
    execDb.activateOnCurrentThread();

    for (OLiveQueryHookV2.OLiveQueryOp iRecord : iRecords) {
      YTResultInternal record;
      if (iRecord.type == ORecordOperation.CREATED || iRecord.type == ORecordOperation.UPDATED) {
        record = copy(execDb, iRecord.after);
        if (iRecord.type == ORecordOperation.UPDATED) {
          YTResultInternal before = copy(execDb, iRecord.before);
          record.setMetadata(BEFORE_METADATA_KEY, before);
        }
      } else {
        record = copy(execDb, iRecord.before);
        record.setMetadata(BEFORE_METADATA_KEY, record);
      }

      if (filter(record)) {
        switch (iRecord.type) {
          case ORecordOperation.DELETED:
            record.setMetadata(BEFORE_METADATA_KEY, null);
            clientListener.onDelete(execDb, applyProjections(record));
            break;
          case ORecordOperation.UPDATED:
            YTResult before =
                applyProjections((YTResultInternal) record.getMetadata(BEFORE_METADATA_KEY));
            record.setMetadata(BEFORE_METADATA_KEY, null);
            clientListener.onUpdate(execDb, before, applyProjections(record));
            break;
          case ORecordOperation.CREATED:
            clientListener.onCreate(execDb, applyProjections(record));
            break;
        }
      }
    }
    if (clientListener instanceof YTLiveQueryBatchResultListener) {
      ((YTLiveQueryBatchResultListener) clientListener).onBatchEnd(execDb);
    }
  }

  private YTResultInternal applyProjections(YTResultInternal record) {
    var ctx = new BasicCommandContext();
    ctx.setDatabase(execDb);

    if (statement.getProjection() != null) {
      YTResultInternal result =
          (YTResultInternal)
              statement.getProjection().calculateSingle(ctx, record);
      return result;
    }
    return record;
  }

  private boolean filter(YTResult record) {
    // filter by class
    if (className != null) {
      Object filterClass = record.getProperty("@class");
      String recordClassName = String.valueOf(filterClass);
      if (filterClass == null) {
        return false;
      } else if (!(className.equalsIgnoreCase(recordClassName))) {
        YTClass recordClass =
            this.execDb.getMetadata().getImmutableSchemaSnapshot().getClass(recordClassName);
        if (recordClass == null) {
          return false;
        }
        if (!recordClass.getName().equalsIgnoreCase(className)
            && !recordClass.isSubClassOf(className)) {
          return false;
        }
      }
    }
    if (rids != null && rids.size() > 0) {
      boolean found = false;
      for (YTRecordId rid : rids) {
        if (rid.equals(record.getIdentity().orElse(null))) {
          found = true;
          break;
        }
        if (rid.equals(record.getProperty("@rid"))) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    // filter conditions
    SQLWhereClause where = statement.getWhereClause();
    if (where == null) {
      return true;
    }
    BasicCommandContext ctx = new BasicCommandContext();
    ctx.setInputParameters(params);
    return where.matchesFilters(record, ctx);
  }

  private YTResultInternal copy(YTDatabaseSessionInternal db, YTResult item) {
    if (item == null) {
      return null;
    }
    YTResultInternal result = new YTResultInternal(db);

    for (String prop : item.getPropertyNames()) {
      result.setProperty(prop, item.getProperty(prop));
    }
    return result;
  }

  private static Map<Object, Object> toPositionalParams(Object[] iArgs) {
    Map<Object, Object> result = new HashMap<>();
    for (int i = 0; i < iArgs.length; i++) {
      result.put(i, iArgs[i]);
    }
    return result;
  }

  @Override
  public void onLiveResultEnd() {
    clientListener.onEnd(execDb);
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

  public SQLSelectStatement getStatement() {
    return statement;
  }
}
