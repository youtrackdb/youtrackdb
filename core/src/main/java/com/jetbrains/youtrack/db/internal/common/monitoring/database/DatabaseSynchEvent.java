package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.DatabaseSynch")
@Description("Database synch")
@Label("Database synch")
@Category("Database")
@Enabled(false)
public class DatabaseSynchEvent extends jdk.jfr.Event {

  private final String databaseName;

  public DatabaseSynchEvent(String databaseName) {
    this.databaseName = databaseName;
  }
}
