package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.Map;

/**
 *
 */
public interface ScriptExecutor {

  ResultSet execute(DatabaseSessionInternal database, String script, Object... params);

  ResultSet execute(DatabaseSessionInternal database, String script, Map params);

  Object executeFunction(
      CommandContext context, final String functionName, final Map<Object, Object> iArgs);

  void registerInterceptor(ScriptInterceptor interceptor);

  void unregisterInterceptor(ScriptInterceptor interceptor);

  default void close(String iDatabaseName) {
  }

  default void closeAll() {
  }
}
