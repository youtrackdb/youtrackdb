package com.jetbrains.youtrack.db.internal.core.command.traverse;

import com.jetbrains.youtrack.db.internal.core.command.OScriptExecutor;
import com.jetbrains.youtrack.db.internal.core.command.OScriptInterceptor;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import java.util.ArrayList;
import java.util.List;

public abstract class OAbstractScriptExecutor implements OScriptExecutor {

  protected String language;

  public OAbstractScriptExecutor(String language) {
    this.language = language;
  }

  private final List<OScriptInterceptor> interceptors = new ArrayList<>();

  @Override
  public void registerInterceptor(OScriptInterceptor interceptor) {
    interceptors.add(interceptor);
  }

  public void preExecute(YTDatabaseSessionInternal database, String script, Object params) {

    interceptors.forEach(i -> i.preExecute(database, language, script, params));
  }

  @Override
  public void unregisterInterceptor(OScriptInterceptor interceptor) {
    interceptors.remove(interceptor);
  }
}
