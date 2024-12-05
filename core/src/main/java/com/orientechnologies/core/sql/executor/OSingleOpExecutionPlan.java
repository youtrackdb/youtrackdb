package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.parser.OSimpleExecStatement;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class OSingleOpExecutionPlan implements OInternalExecutionPlan {

  protected final OSimpleExecStatement statement;
  private final OCommandContext ctx;

  private boolean executed = false;
  private OExecutionStream result;

  public OSingleOpExecutionPlan(OCommandContext ctx, OSimpleExecStatement stm) {
    this.ctx = ctx;
    this.statement = stm;
  }

  @Override
  public OCommandContext getContext() {
    return ctx;
  }

  @Override
  public void close() {
  }

  @Override
  public OExecutionStream start() {
    if (executed && result == null) {
      return OExecutionStream.empty();
    }
    if (!executed) {
      executed = true;
      result = statement.executeSimple(this.ctx);
    }
    return result;
  }

  public void reset(OCommandContext ctx) {
    executed = false;
  }

  @Override
  public long getCost() {
    return 0;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  public OExecutionStream executeInternal(OBasicCommandContext ctx)
      throws YTCommandExecutionException {
    if (executed) {
      throw new YTCommandExecutionException(
          "Trying to execute a result-set twice. Please use reset()");
    }
    executed = true;
    result = statement.executeSimple(this.ctx);
    return result;
  }

  @Override
  public List<OExecutionStep> getSteps() {
    return Collections.emptyList();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ " + statement.toString();
    return result;
  }

  @Override
  public YTResult toResult(YTDatabaseSessionInternal db) {
    YTResultInternal result = new YTResultInternal(db);
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("stmText", statement.toString());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", null);
    return result;
  }
}
