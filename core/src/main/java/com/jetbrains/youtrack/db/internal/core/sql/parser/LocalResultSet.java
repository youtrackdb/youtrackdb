package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.api.query.ExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.InternalExecutionPlan;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class LocalResultSet implements ResultSet {

  private ExecutionStream stream = null;
  private final InternalExecutionPlan executionPlan;

  long totalExecutionTime = 0;
  long startTime = 0;

  public LocalResultSet(InternalExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
    start();
  }

  private boolean start() {
    long begin = System.currentTimeMillis();
    try {
      if (stream == null) {
        startTime = begin;
      }
      stream = executionPlan.start();
      if (!stream.hasNext(executionPlan.getContext())) {
        logProfiling();
        return false;
      }
      return true;
    } finally {
      totalExecutionTime += (System.currentTimeMillis() - begin);
    }
  }

  @Override
  public boolean hasNext() {
    boolean next = stream.hasNext(executionPlan.getContext());
    if (!next) {
      logProfiling();
    }
    return next;
  }

  @Override
  public Result next() {
    if (!hasNext()) {
      throw new IllegalStateException();
    }
    return stream.next(executionPlan.getContext());
  }

  private void logProfiling() {
    if (executionPlan.getStatement() != null && YouTrackDBEnginesManager.instance().getProfiler()
        .isRecording()) {
      final DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().getIfDefined();
      if (db != null) {
        final SecurityUser user = db.geCurrentUser();
        final String userString = user != null ? user.toString() : null;
        YouTrackDBEnginesManager.instance()
            .getProfiler()
            .stopChrono(
                "db."
                    + DatabaseRecordThreadLocal.instance().get().getName()
                    + ".command.sql."
                    + executionPlan.getStatement(),
                "Command executed against the database",
                System.currentTimeMillis() - totalExecutionTime,
                "db.*.command.*",
                null,
                userString);
      }
    }
  }

  public long getTotalExecutionTime() {
    return totalExecutionTime;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public void close() {
    stream.close(executionPlan.getContext());
    executionPlan.close();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.of(executionPlan);
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>(); // TODO
  }
}
