package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.Transaction")
@Label("Database transaction")
@Description("Any database transaction (read or write)")
@Category({"Database", "Transaction"})
@Enabled(false)
public class TransactionEvent extends jdk.jfr.Event {
}
