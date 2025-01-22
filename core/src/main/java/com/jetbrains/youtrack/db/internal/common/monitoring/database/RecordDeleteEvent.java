package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.RecordDelete")
@Description("Deletion of a database record")
@Label("Record deletion")
@Category({"Database", "Record"})
@Enabled(false)
public class RecordDeleteEvent extends jdk.jfr.Event {

  private final String databaseName;
  private boolean conflict;

  public RecordDeleteEvent(String databaseName) {
    this.databaseName = databaseName;
  }

  public void setConflict(boolean conflict) {
    this.conflict = conflict;
  }
}
