package com.jetbrains.youtrack.db.internal.common.monitoring.database;


import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.FunctionExecution")
@Description("Function execution")
@Label("Function execution")
@Category("Database")
@Enabled(false)
public class FunctionExecuteEvent extends jdk.jfr.Event {

  private final String databaseName;

  public FunctionExecuteEvent(String databaseName) {
    this.databaseName = databaseName;
  }
}
