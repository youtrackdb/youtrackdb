package com.orientechnologies.core.command;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;

public interface OScriptInterceptor {

  void preExecute(YTDatabaseSessionInternal database, String language, String script,
      Object params);
}
