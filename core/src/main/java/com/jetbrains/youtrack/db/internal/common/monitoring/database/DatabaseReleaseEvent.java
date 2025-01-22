package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.DatabaseRelease")
@Description("Database release (after freeze)")
@Label("Database release")
@Category("Database")
@Enabled(false)
public class DatabaseReleaseEvent extends jdk.jfr.Event {
  private final String databaseName;

  public DatabaseReleaseEvent(String databaseName) {
    this.databaseName = databaseName;
  }
}
