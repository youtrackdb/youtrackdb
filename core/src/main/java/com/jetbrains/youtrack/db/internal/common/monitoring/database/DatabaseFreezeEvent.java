package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.DatabaseFreeze")
@Description("Database freeze")
@Label("Database freeze")
@Category("Database")
@Enabled(false)
public class DatabaseFreezeEvent extends jdk.jfr.Event {
  private final String databaseName;

  public DatabaseFreezeEvent(String databaseName) {
    this.databaseName = databaseName;
  }
}
