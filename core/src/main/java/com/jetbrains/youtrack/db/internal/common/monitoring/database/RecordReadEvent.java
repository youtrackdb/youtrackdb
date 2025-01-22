package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.RecordCreate")
@Description("Creation of a database record")
@Label("Record creation")
@Category({"Database", "Record"})
@Enabled(false)
public class RecordReadEvent extends jdk.jfr.Event {

  private final String databaseName;

  public RecordReadEvent(String databaseName) {
    this.databaseName = databaseName;
  }
}
