package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.DatabaseDrop")
@Description("Database drop")
@Label("Database drop")
@Category("Database")
@Enabled(false)
public class DatabaseDropEvent extends jdk.jfr.Event {

  private final String databaseName;

  public DatabaseDropEvent(String databaseName) {
    this.databaseName = databaseName;
  }
}
