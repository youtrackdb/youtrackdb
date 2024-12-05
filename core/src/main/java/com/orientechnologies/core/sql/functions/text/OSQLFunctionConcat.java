package com.orientechnologies.core.sql.functions.text;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.sql.functions.OSQLFunctionConfigurableAbstract;

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
      OCommandContext iContext) {
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
