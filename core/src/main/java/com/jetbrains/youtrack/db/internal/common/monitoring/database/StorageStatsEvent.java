package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.DatabaseStats")
@Description("Database statistics")
@Label("Database statistics")
@Category("Database")
@Enabled(false)
public class StorageStatsEvent extends jdk.jfr.Event {

  private final String storageName;
  private final long sizeOnDisk;

  public StorageStatsEvent(String storageName, long sizeOnDisk) {
    this.storageName = storageName;
    this.sizeOnDisk = sizeOnDisk;
  }
}
