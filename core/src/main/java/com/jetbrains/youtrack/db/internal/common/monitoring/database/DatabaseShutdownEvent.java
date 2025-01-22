package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.DatabaseShutdown")
@Description("Database shutdown")
@Label("Database shutdown")
@Category("Database")
@Enabled(false)
public class DatabaseShutdownEvent extends jdk.jfr.Event {
  private final String databaseName;

  public DatabaseShutdownEvent(String databaseName) {
    this.databaseName = databaseName;
  }
}
