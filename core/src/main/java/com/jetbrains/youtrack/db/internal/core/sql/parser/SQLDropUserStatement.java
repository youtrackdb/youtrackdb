/* Generated By:JJTree: Do not edit this line. SQLDropUserStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SQLDropUserStatement extends SQLSimpleExecStatement {

  public SQLDropUserStatement(int id) {
    super(id);
  }

  public SQLDropUserStatement(YouTrackDBSql p, int id) {
    super(p, id);
  }

  protected SQLIdentifier name;

  @Override
  public ExecutionStream executeSimple(CommandContext ctx) {

    List<Object> params = new ArrayList<>();

    var sb = "DELETE FROM OUser WHERE " + SQLCreateUserStatement.USER_FIELD_NAME + " = ?";
    params.add(this.name.getStringValue());

    return ExecutionStream.resultIterator(
        ctx.getDatabaseSession().command(sb, params.toArray()).stream().iterator());
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("DROP USER ");
    name.toString(params, builder);
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("DROP USER ");
    name.toGenericStatement(builder);
  }

  @Override
  public SQLDropUserStatement copy() {
    var result = new SQLDropUserStatement(-1);
    result.name = name == null ? null : name.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    var that = (SQLDropUserStatement) o;

    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return name != null ? name.hashCode() : 0;
  }
}
/* JavaCC - OriginalChecksum=af06c39c521df6d625e8e548b11e22c8 (do not edit this line) */
