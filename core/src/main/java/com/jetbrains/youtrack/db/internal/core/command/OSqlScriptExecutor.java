package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.command.script.CommandExecutorFunction;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandFunction;
import com.jetbrains.youtrack.db.internal.core.command.traverse.OAbstractScriptExecutor;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.YTCommandSQLParsingException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OInternalExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ORetryExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OScriptExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.RetryStep;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBeginStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCommitStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLLetStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.YTLocalResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class OSqlScriptExecutor extends OAbstractScriptExecutor {

  public OSqlScriptExecutor() {
    super("SQL");
  }

  @Override
  public YTResultSet execute(YTDatabaseSessionInternal database, String script, Object... args)
      throws YTCommandSQLParsingException, YTCommandExecutionException {

    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<SQLStatement> statements = OSQLEngine.parseScript(script, database);

    CommandContext scriptContext = new BasicCommandContext();
    scriptContext.setDatabase(database);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    scriptContext.setInputParameters(params);

    return executeInternal(statements, scriptContext);
  }

  @Override
  public YTResultSet execute(YTDatabaseSessionInternal database, String script, Map params) {
    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<SQLStatement> statements = OSQLEngine.parseScript(script, database);

    CommandContext scriptContext = new BasicCommandContext();
    scriptContext.setDatabase(database);

    scriptContext.setInputParameters(params);

    return executeInternal(statements, scriptContext);
  }

  private YTResultSet executeInternal(List<SQLStatement> statements, CommandContext scriptContext) {
    OScriptExecutionPlan plan = new OScriptExecutionPlan(scriptContext);

    plan.setStatement(
        statements.stream().map(SQLStatement::toString).collect(Collectors.joining(";")));

    List<SQLStatement> lastRetryBlock = new ArrayList<>();
    int nestedTxLevel = 0;

    for (SQLStatement stm : statements) {
      if (stm.getOriginalStatement() == null) {
        stm.setOriginalStatement(stm.toString());
      }
      if (stm instanceof SQLBeginStatement) {
        nestedTxLevel++;
      }

      if (nestedTxLevel <= 0) {
        OInternalExecutionPlan sub = stm.createExecutionPlan(scriptContext);
        plan.chain(sub, false);
      } else {
        lastRetryBlock.add(stm);
      }

      if (stm instanceof SQLCommitStatement && nestedTxLevel > 0) {
        nestedTxLevel--;
        if (nestedTxLevel == 0) {
          if (((SQLCommitStatement) stm).getRetry() != null) {
            int nRetries = ((SQLCommitStatement) stm).getRetry().getValue().intValue();
            if (nRetries <= 0) {
              throw new YTCommandExecutionException("Invalid retry number: " + nRetries);
            }

            RetryStep step =
                new RetryStep(
                    lastRetryBlock,
                    nRetries,
                    ((SQLCommitStatement) stm).getElseStatements(),
                    ((SQLCommitStatement) stm).getElseFail(),
                    scriptContext,
                    false);
            ORetryExecutionPlan retryPlan = new ORetryExecutionPlan(scriptContext);
            retryPlan.chain(step);
            plan.chain(retryPlan, false);
            lastRetryBlock = new ArrayList<>();
          } else {
            for (SQLStatement statement : lastRetryBlock) {
              OInternalExecutionPlan sub = statement.createExecutionPlan(scriptContext);
              plan.chain(sub, false);
            }
          }
        }
      }

      if (stm instanceof SQLLetStatement) {
        scriptContext.declareScriptVariable(((SQLLetStatement) stm).getName().getStringValue());
      }
    }
    return new YTLocalResultSet(plan);
  }

  @Override
  public Object executeFunction(
      CommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    final CommandExecutorFunction command = new CommandExecutorFunction();
    command.parse(new CommandFunction(functionName));
    return command.executeInContext(context, iArgs);
  }
}
