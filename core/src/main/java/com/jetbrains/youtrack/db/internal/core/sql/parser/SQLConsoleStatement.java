/* Generated By:JJTree: Do not edit this line. SQLConsoleStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.Map;
import java.util.Objects;

public class SQLConsoleStatement extends SQLSimpleExecStatement {

  protected SQLIdentifier logLevel;
  protected SQLExpression message;

  public SQLConsoleStatement(int id) {
    super(id);
  }

  public SQLConsoleStatement(YouTrackDBSql p, int id) {
    super(p, id);
  }

  @Override
  public ExecutionStream executeSimple(CommandContext ctx) {
    var item = new ResultInternal(ctx.getDatabaseSession());
    Object msg = "" + message.execute((Identifiable) null, ctx);

    if (logLevel.getStringValue().equalsIgnoreCase("log")) {
      LogManager.instance().info(this, "%s", msg);
    } else if (logLevel.getStringValue().equalsIgnoreCase("output")) {
      System.out.println(msg);
    } else if (logLevel.getStringValue().equalsIgnoreCase("error")) {
      System.err.println(msg);
      LogManager.instance().error(this, "%s", null, msg);
    } else if (logLevel.getStringValue().equalsIgnoreCase("warn")) {
      LogManager.instance().warn(this, "%s", msg);
    } else if (logLevel.getStringValue().equalsIgnoreCase("debug")) {
      LogManager.instance().debug(this, "%s", msg);
    } else {
      throw new CommandExecutionException(ctx.getDatabaseSession(),
          "Unsupported log level: " + logLevel);
    }

    item.setProperty("operation", "console");
    item.setProperty("level", logLevel.getStringValue());
    item.setProperty("message", msg);
    return ExecutionStream.singleton(item);
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("CONSOLE.");
    logLevel.toString(params, builder);
    builder.append(" ");
    message.toString(params, builder);
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("CONSOLE.");
    logLevel.toGenericStatement(builder);
    builder.append(" ");
    message.toGenericStatement(builder);
  }

  @Override
  public SQLConsoleStatement copy() {
    var result = new SQLConsoleStatement(-1);
    result.logLevel = logLevel == null ? null : logLevel.copy();
    result.message = message == null ? null : message.copy();
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

    var that = (SQLConsoleStatement) o;

    if (!Objects.equals(logLevel, that.logLevel)) {
      return false;
    }
    return Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    var result = logLevel != null ? logLevel.hashCode() : 0;
    result = 31 * result + (message != null ? message.hashCode() : 0);
    return result;
  }
}
/* JavaCC - OriginalChecksum=626c09cda52a1a8a63eeefcb37bd66a1 (do not edit this line) */
