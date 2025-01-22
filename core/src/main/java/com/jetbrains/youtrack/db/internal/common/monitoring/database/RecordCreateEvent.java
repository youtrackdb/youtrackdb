package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.RecordRead")
@Description("Read of a database record")
@Label("Record read")
@Category({"Database", "Record"})
@Enabled(false)
public class RecordCreateEvent extends jdk.jfr.Event {

  private final String databaseName;

  public RecordCreateEvent(String databaseName) {
    this.databaseName = databaseName;
  }
}
