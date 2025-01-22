package com.jetbrains.youtrack.db.internal.common.monitoring.database;


import jdk.jfr.Category;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.DatabaseTip")
@Label("Database tip")
@Category("Database")
@Enabled(false)
public class DatabaseTipEvent extends jdk.jfr.Event {

  private final String tip;

  public DatabaseTipEvent(String tip, Object... args) {
    if (isEnabled()) {
      tip = String.format(tip, args);
    }
    this.tip = tip;
  }
}
