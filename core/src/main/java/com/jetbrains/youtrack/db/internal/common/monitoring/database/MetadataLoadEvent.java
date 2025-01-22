package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.MetadataLoad")
@Description("Loading of database metadata")
@Label("Metadata load")
@Category("Database")
@Enabled(false)
public class MetadataLoadEvent extends jdk.jfr.Event {

  private final String databaseName;

  public MetadataLoadEvent(String databaseName) {
    this.databaseName = databaseName;
  }
}
