package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import com.jetbrains.youtrack.db.internal.core.index.Index;
import jdk.jfr.Category;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.QueryIndexUsed")
@Label("Index usage")
@Category({"Database", "Query"})
@Enabled(false)
public class QueryIndexUsedEvent extends jdk.jfr.Event {

  private final String databaseName;
  private final int paramCount;
  private final int keyCount;

  public QueryIndexUsedEvent(String databaseName, int paramCount, int keyCount) {
    this.databaseName = databaseName;
    this.paramCount = paramCount;
    this.keyCount = keyCount;
  }

  public static void fire(Index index) {
    fire(index.getDatabaseName(), index.getDefinition().getParamCount(), 0);
  }

  public static void fire(Index index, int keyCount) {
    fire(index.getDatabaseName(), index.getDefinition().getParamCount(), keyCount);
  }

  public static void fire(String databaseName) {
    fire(databaseName, 0, 0);
  }

  public static void fire(String databaseName, int paramCount, int keyCount) {
    new QueryIndexUsedEvent(normalizeDbName(databaseName), paramCount, keyCount).commit();
  }

  private static String normalizeDbName(String databaseName) {
    return databaseName == null ? "*" : databaseName;
  }
}
