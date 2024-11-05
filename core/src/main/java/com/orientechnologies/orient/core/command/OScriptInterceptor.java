package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;

public interface OScriptInterceptor {

  void preExecute(ODatabaseSessionInternal database, String language, String script, Object params);
}
