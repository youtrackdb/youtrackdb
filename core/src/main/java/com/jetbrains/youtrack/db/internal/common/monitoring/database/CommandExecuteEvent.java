package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.CommandExecution")
@Description("Command execution")
@Label("Command execution")
@Category("Database")
@Enabled(false)
public class CommandExecuteEvent extends jdk.jfr.Event {

  private String databaseName;
  private String command;
  private String user;

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public void setUser(String user) {
    this.user = user;
  }
}
