package com.orientechnologies.core.sql.parser;

import com.orientechnologies.core.YouTrackDBManager;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.metadata.security.YTSecurityUser;
import com.orientechnologies.core.sql.executor.OExecutionPlan;
import com.orientechnologies.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class YTLocalResultSet implements YTResultSet {

  private OExecutionStream stream = null;
  private final OInternalExecutionPlan executionPlan;

  long totalExecutionTime = 0;
  long startTime = 0;

  public YTLocalResultSet(OInternalExecutionPlan executionPlan) {
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
  public YTResult next() {
    if (!hasNext()) {
      throw new IllegalStateException();
    }
    return stream.next(executionPlan.getContext());
  }

  private void logProfiling() {
    if (executionPlan.getStatement() != null && YouTrackDBManager.instance().getProfiler()
        .isRecording()) {
      final YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      if (db != null) {
        final YTSecurityUser user = db.getUser();
        final String userString = user != null ? user.toString() : null;
        YouTrackDBManager.instance()
            .getProfiler()
            .stopChrono(
                "db."
                    + ODatabaseRecordThreadLocal.instance().get().getName()
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
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.of(executionPlan);
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>(); // TODO
  }
}
