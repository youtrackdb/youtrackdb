/* Generated By:JJTree: Do not edit this line. SQLRebuildIndexStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.Map;
import java.util.Objects;

public class SQLRebuildIndexStatement extends SQLSimpleExecStatement {

  protected boolean all = false;
  protected SQLIndexName name;

  public SQLRebuildIndexStatement(int id) {
    super(id);
  }

  public SQLRebuildIndexStatement(YouTrackDBSql p, int id) {
    super(p, id);
  }

  @Override
  public ExecutionStream executeSimple(CommandContext ctx) {
    final var session = ctx.getDatabaseSession();
    var result = new ResultInternal(session);
    result.setProperty("operation", "rebuild index");

    if (all) {
      long totalIndexed = 0;
      for (var idx : session.getMetadata().getIndexManagerInternal().getIndexes(session)) {
        if (idx.isAutomatic()) {
          totalIndexed += idx.rebuild(session);
        }
      }

      result.setProperty("totalIndexed", totalIndexed);
    } else {
      final var idx =
          session.getMetadata().getIndexManagerInternal().getIndex(session, name.getValue());
      if (idx == null) {
        throw new CommandExecutionException(session, "Index '" + name + "' not found");
      }

      if (!idx.isAutomatic()) {
        throw new CommandExecutionException(session,
            "Cannot rebuild index '"
                + name
                + "' because it's manual and there aren't indications of what to index");
      }

      var val = idx.rebuild(session);
      result.setProperty("totalIndexed", val);
    }
    return ExecutionStream.singleton(result);
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("REBUILD INDEX ");
    if (all) {
      builder.append("*");
    } else {
      name.toString(params, builder);
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("REBUILD INDEX ");
    if (all) {
      builder.append("*");
    } else {
      name.toGenericStatement(builder);
    }
  }

  @Override
  public SQLRebuildIndexStatement copy() {
    var result = new SQLRebuildIndexStatement(-1);
    result.all = all;
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

    var that = (SQLRebuildIndexStatement) o;

    if (all != that.all) {
      return false;
    }
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    var result = (all ? 1 : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
/* JavaCC - OriginalChecksum=baca3c54112f1c08700ebdb691fa85bd (do not edit this line) */
