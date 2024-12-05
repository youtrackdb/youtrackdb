package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.util.Map;

/**
 *
 */
public interface OScriptExecutor {

  YTResultSet execute(YTDatabaseSessionInternal database, String script, Object... params);

  YTResultSet execute(YTDatabaseSessionInternal database, String script, Map params);

  Object executeFunction(
      CommandContext context, final String functionName, final Map<Object, Object> iArgs);

  void registerInterceptor(OScriptInterceptor interceptor);

  void unregisterInterceptor(OScriptInterceptor interceptor);

  default void close(String iDatabaseName) {
  }

  default void closeAll() {
  }
}
