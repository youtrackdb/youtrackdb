package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

public interface ScriptInterceptor {

  void preExecute(DatabaseSessionInternal database, String language, String script,
      Object params);
}
