/* Generated By:JJTree: Do not edit this line. OWhileBlock.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.sql.executor.OForEachExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.OUpdateExecutionPlan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class OWhileBlock extends OStatement {

  protected OBooleanExpression condition;
  protected List<OStatement> statements = new ArrayList<OStatement>();

  public OWhileBlock(int id) {
    super(id);
  }

  public OWhileBlock(OrientSql p, int id) {
    super(p, id);
  }

  public void addStatement(OStatement statement) {
    if (statements == null) {
      this.statements = new ArrayList<>();
    }
    this.statements.add(statement);
  }

  @Override
  public OResultSet execute(
      ODatabaseSessionInternal db, Object[] args, OCommandContext parentCtx, boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setDatabase(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);

    OUpdateExecutionPlan executionPlan;
    if (usePlanCache) {
      executionPlan = createExecutionPlan(ctx, false);
    } else {
      executionPlan = (OUpdateExecutionPlan) createExecutionPlanNoCache(ctx, false);
    }

    executionPlan.executeInternal();
    return new OLocalResultSet(executionPlan);
  }

  @Override
  public OResultSet execute(
      ODatabaseSessionInternal db, Map params, OCommandContext parentCtx, boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);

    OUpdateExecutionPlan executionPlan;
    if (usePlanCache) {
      executionPlan = createExecutionPlan(ctx, false);
    } else {
      executionPlan = (OUpdateExecutionPlan) createExecutionPlanNoCache(ctx, false);
    }

    executionPlan.executeInternal();
    return new OLocalResultSet(executionPlan);
  }

  public OUpdateExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OForEachExecutionPlan plan = new OForEachExecutionPlan(ctx);
    plan.chain(new WhileStep(condition, statements, ctx, enableProfiling));
    return plan;
  }

  @Override
  public OStatement copy() {
    OWhileBlock result = new OWhileBlock(-1);
    result.condition = condition.copy();
    result.statements = statements.stream().map(x -> x.copy()).collect(Collectors.toList());
    return result;
  }

  public boolean containsReturn() {
    for (OStatement stm : this.statements) {
      if (stm instanceof OReturnStatement) {
        return true;
      }
      if (stm instanceof OForEachBlock && ((OForEachBlock) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof OIfStatement && ((OIfStatement) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof OWhileBlock && ((OWhileBlock) stm).containsReturn()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OWhileBlock that = (OWhileBlock) o;

    if (!Objects.equals(condition, that.condition)) {
      return false;
    }
    return Objects.equals(statements, that.statements);
  }

  @Override
  public int hashCode() {
    int result = condition != null ? condition.hashCode() : 0;
    result = 31 * result + (statements != null ? statements.hashCode() : 0);
    return result;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("WHILE (");
    condition.toString(params, builder);
    builder.append(") {\n");
    for (OStatement stm : statements) {
      stm.toString(params, builder);
      builder.append("\n");
    }
    builder.append("}");
  }

  public void toGenericStatement(StringBuilder builder) {
    builder.append("WHILE (");
    condition.toGenericStatement(builder);
    builder.append(") {\n");
    for (OStatement stm : statements) {
      stm.toGenericStatement(builder);
      builder.append("\n");
    }
    builder.append("}");
  }
}
/* JavaCC - OriginalChecksum=1b38ee666f89790d0f54cc5823b99286 (do not edit this line) */
