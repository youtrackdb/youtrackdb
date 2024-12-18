package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandExecutorFunction;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandFunction;
import com.jetbrains.youtrack.db.internal.core.command.traverse.AbstractScriptExecutor;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.executor.InternalExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.RetryExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.RetryStep;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ScriptExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.parser.LocalResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBeginStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCommitStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLLetStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class SqlScriptExecutor extends AbstractScriptExecutor {

  public SqlScriptExecutor() {
    super("SQL");
  }

  @Override
  public ResultSet execute(DatabaseSessionInternal database, String script, Object... args)
      throws CommandSQLParsingException, CommandExecutionException {

    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<SQLStatement> statements = SQLEngine.parseScript(script, database);

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
  public ResultSet execute(DatabaseSessionInternal database, String script, Map params) {
    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<SQLStatement> statements = SQLEngine.parseScript(script, database);

    CommandContext scriptContext = new BasicCommandContext();
    scriptContext.setDatabase(database);

    scriptContext.setInputParameters(params);

    return executeInternal(statements, scriptContext);
  }

  private ResultSet executeInternal(List<SQLStatement> statements, CommandContext scriptContext) {
    ScriptExecutionPlan plan = new ScriptExecutionPlan(scriptContext);

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
        InternalExecutionPlan sub = stm.createExecutionPlan(scriptContext);
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
              throw new CommandExecutionException("Invalid retry number: " + nRetries);
            }

            RetryStep step =
                new RetryStep(
                    lastRetryBlock,
                    nRetries,
                    ((SQLCommitStatement) stm).getElseStatements(),
                    ((SQLCommitStatement) stm).getElseFail(),
                    scriptContext,
                    false);
            RetryExecutionPlan retryPlan = new RetryExecutionPlan(scriptContext);
            retryPlan.chain(step);
            plan.chain(retryPlan, false);
            lastRetryBlock = new ArrayList<>();
          } else {
            for (SQLStatement statement : lastRetryBlock) {
              InternalExecutionPlan sub = statement.createExecutionPlan(scriptContext);
              plan.chain(sub, false);
            }
          }
        }
      }

      if (stm instanceof SQLLetStatement) {
        scriptContext.declareScriptVariable(((SQLLetStatement) stm).getName().getStringValue());
      }
    }
    return new LocalResultSet(plan);
  }

  @Override
  public Object executeFunction(
      CommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    final CommandExecutorFunction command = new CommandExecutorFunction();
    command.parse(context.getDatabase(), new CommandFunction(functionName));
    return command.executeInContext(context, iArgs);
  }
}
