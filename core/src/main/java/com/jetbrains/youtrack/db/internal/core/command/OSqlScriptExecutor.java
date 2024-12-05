package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.command.script.OCommandExecutorFunction;
import com.jetbrains.youtrack.db.internal.core.command.script.OCommandFunction;
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
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBeginStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OCommitStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OLetStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OStatement;
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
    List<OStatement> statements = OSQLEngine.parseScript(script, database);

    OCommandContext scriptContext = new OBasicCommandContext();
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
    List<OStatement> statements = OSQLEngine.parseScript(script, database);

    OCommandContext scriptContext = new OBasicCommandContext();
    scriptContext.setDatabase(database);

    scriptContext.setInputParameters(params);

    return executeInternal(statements, scriptContext);
  }

  private YTResultSet executeInternal(List<OStatement> statements, OCommandContext scriptContext) {
    OScriptExecutionPlan plan = new OScriptExecutionPlan(scriptContext);

    plan.setStatement(
        statements.stream().map(OStatement::toString).collect(Collectors.joining(";")));

    List<OStatement> lastRetryBlock = new ArrayList<>();
    int nestedTxLevel = 0;

    for (OStatement stm : statements) {
      if (stm.getOriginalStatement() == null) {
        stm.setOriginalStatement(stm.toString());
      }
      if (stm instanceof OBeginStatement) {
        nestedTxLevel++;
      }

      if (nestedTxLevel <= 0) {
        OInternalExecutionPlan sub = stm.createExecutionPlan(scriptContext);
        plan.chain(sub, false);
      } else {
        lastRetryBlock.add(stm);
      }

      if (stm instanceof OCommitStatement && nestedTxLevel > 0) {
        nestedTxLevel--;
        if (nestedTxLevel == 0) {
          if (((OCommitStatement) stm).getRetry() != null) {
            int nRetries = ((OCommitStatement) stm).getRetry().getValue().intValue();
            if (nRetries <= 0) {
              throw new YTCommandExecutionException("Invalid retry number: " + nRetries);
            }

            RetryStep step =
                new RetryStep(
                    lastRetryBlock,
                    nRetries,
                    ((OCommitStatement) stm).getElseStatements(),
                    ((OCommitStatement) stm).getElseFail(),
                    scriptContext,
                    false);
            ORetryExecutionPlan retryPlan = new ORetryExecutionPlan(scriptContext);
            retryPlan.chain(step);
            plan.chain(retryPlan, false);
            lastRetryBlock = new ArrayList<>();
          } else {
            for (OStatement statement : lastRetryBlock) {
              OInternalExecutionPlan sub = statement.createExecutionPlan(scriptContext);
              plan.chain(sub, false);
            }
          }
        }
      }

      if (stm instanceof OLetStatement) {
        scriptContext.declareScriptVariable(((OLetStatement) stm).getName().getStringValue());
      }
    }
    return new YTLocalResultSet(plan);
  }

  @Override
  public Object executeFunction(
      OCommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    final OCommandExecutorFunction command = new OCommandExecutorFunction();
    command.parse(new OCommandFunction(functionName));
    return command.executeInContext(context, iArgs);
  }
}
