package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.TransactionWrite")
@Description("Write database transaction (commited or rolled back)")
@Label("Write database transaction")
@Category({"Database", "Transactions"})
@Enabled(false)
public class TransactionWriteEvent extends jdk.jfr.Event {

  private final boolean successful;

  public TransactionWriteEvent(boolean successful) {
    this.successful = successful;
  }
}
