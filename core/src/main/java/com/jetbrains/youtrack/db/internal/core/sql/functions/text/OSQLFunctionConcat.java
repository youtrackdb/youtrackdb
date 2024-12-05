package com.jetbrains.youtrack.db.internal.core.sql.functions.text;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionConfigurableAbstract;

public class OSQLFunctionConcat extends OSQLFunctionConfigurableAbstract {

  public static final String NAME = "concat";
  private StringBuilder sb;

  public OSQLFunctionConcat() {
    super(NAME, 1, 2);
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      CommandContext iContext) {
    if (sb == null) {
      sb = new StringBuilder();
    } else {
      if (iParams.length > 1) {
        sb.append(iParams[1]);
      }
    }
    sb.append(iParams[0]);
    return null;
  }

  @Override
  public Object getResult() {
    return sb != null ? sb.toString() : null;
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return "concat(<field>, [<delim>])";
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }
}
