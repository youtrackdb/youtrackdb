package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;

public interface OScriptInterceptor {

  void preExecute(YTDatabaseSessionInternal database, String language, String script,
      Object params);
}
