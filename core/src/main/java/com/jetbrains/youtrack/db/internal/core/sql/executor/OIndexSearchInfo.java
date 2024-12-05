package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;

public class OIndexSearchInfo {

  private final boolean allowsRangeQueries;
  private final boolean map;
  private final boolean indexByKey;
  private final String field;
  private final OCommandContext ctx;
  private final boolean indexByValue;

  public OIndexSearchInfo(
      String indexField,
      boolean allowsRangeQueries,
      boolean map,
      boolean indexByKey,
      boolean indexByValue,
      OCommandContext ctx) {
    this.field = indexField;
    this.allowsRangeQueries = allowsRangeQueries;
    this.map = map;
    this.indexByKey = indexByKey;
    this.ctx = ctx;
    this.indexByValue = indexByValue;
  }

  public String getField() {
    return field;
  }

  public OCommandContext getCtx() {
    return ctx;
  }

  public boolean allowsRange() {
    return allowsRangeQueries;
  }

  public boolean isMap() {
    return map;
  }

  public boolean isIndexByKey() {
    return indexByKey;
  }

  public boolean isIndexByValue() {
    return indexByValue;
  }
}
