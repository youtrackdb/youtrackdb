package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;

public interface OScriptInterceptor {

  void preExecute(YTDatabaseSessionInternal database, String language, String script,
      Object params);
}
