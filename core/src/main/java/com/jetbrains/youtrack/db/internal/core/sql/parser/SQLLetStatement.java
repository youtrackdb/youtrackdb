/* Generated By:JJTree: Do not edit this line. SQLLetStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.InternalResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.Map;
import java.util.Objects;

public class SQLLetStatement extends SQLSimpleExecStatement {

  protected SQLIdentifier name;

  protected SQLStatement statement;
  protected SQLExpression expression;

  public SQLLetStatement(int id) {
    super(id);
  }

  public SQLLetStatement(YouTrackDBSql p, int id) {
    super(p, id);
  }

  @Override
  public ExecutionStream executeSimple(CommandContext ctx) {
    Object result;
    if (expression != null) {
      result = expression.execute((Result) null, ctx);
    } else {
      var params = ctx.getInputParameters();
      if (statement.originalStatement == null) {
        statement.setOriginalStatement(statement.toString());
      }
      result = statement.execute(ctx.getDatabaseSession(), params, ctx, false);
    }
    var session = ctx.getDatabaseSession();
    if (result instanceof ResultSet) {
      var rs = new InternalResultSet(session);
      ((ResultSet) result).stream().forEach(rs::add);
      rs.setPlan(((ResultSet) result).getExecutionPlan().orElse(null));
      ((ResultSet) result).close();
      result = rs;
    }

    if (ctx.getParent() != null) {
      ctx.getParent().setVariable(name.getStringValue(), result);
    } else {
      ctx.setVariable(name.getStringValue(), result);
    }
    return ExecutionStream.empty();
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("LET ");
    name.toString(params, builder);
    builder.append(" = ");
    if (statement != null) {
      statement.toString(params, builder);
    } else {
      expression.toString(params, builder);
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("LET ");
    name.toGenericStatement(builder);
    builder.append(" = ");
    if (statement != null) {
      statement.toGenericStatement(builder);
    } else {
      expression.toGenericStatement(builder);
    }
  }

  @Override
  public SQLLetStatement copy() {
    var result = new SQLLetStatement(-1);
    result.name = name == null ? null : name.copy();
    result.statement = statement == null ? null : statement.copy();
    result.expression = expression == null ? null : expression.copy();
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

    var that = (SQLLetStatement) o;

    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(statement, that.statement)) {
      return false;
    }
    return Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    var result = name != null ? name.hashCode() : 0;
    result = 31 * result + (statement != null ? statement.hashCode() : 0);
    result = 31 * result + (expression != null ? expression.hashCode() : 0);
    return result;
  }

  public SQLIdentifier getName() {
    return name;
  }
}
/* JavaCC - OriginalChecksum=cc646e5449351ad9ced844f61b687928 (do not edit this line) */
