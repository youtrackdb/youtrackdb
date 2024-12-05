package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class OSelectExecutionPlan implements OInternalExecutionPlan {

  private String location;

  protected CommandContext ctx;

  protected List<ExecutionStepInternal> steps = new ArrayList<>();

  private ExecutionStepInternal lastStep = null;

  private String statement;
  private String genericStatement;

  public OSelectExecutionPlan(CommandContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public CommandContext getContext() {
    return ctx;
  }

  @Override
  public void close() {
    lastStep.close();
  }

  @Override
  public ExecutionStream start() {
    return lastStep.start(ctx);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < steps.size(); i++) {
      ExecutionStepInternal step = steps.get(i);
      result.append(step.prettyPrint(depth, indent));
      if (i < steps.size() - 1) {
        result.append("\n");
      }
    }
    return result.toString();
  }

  @Override
  public void reset(CommandContext ctx) {
    steps.forEach(ExecutionStepInternal::reset);
  }

  public void chain(ExecutionStepInternal nextStep) {
    if (lastStep != null) {
      lastStep.setNext(nextStep);
      nextStep.setPrevious(lastStep);
    }
    lastStep = nextStep;
    steps.add(nextStep);
  }

  @Override
  public List<ExecutionStep> getSteps() {
    // TODO do a copy of the steps
    return (List) steps;
  }

  public void setSteps(List<ExecutionStepInternal> steps) {
    this.steps = steps;
    if (steps.size() > 0) {
      lastStep = steps.get(steps.size() - 1);
    } else {
      lastStep = null;
    }
  }

  @Override
  public YTResult toResult(YTDatabaseSessionInternal db) {
    YTResultInternal result = new YTResultInternal(db);
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty(
        "steps",
        steps == null ? null
            : steps.stream().map(x -> x.toResult(db)).collect(Collectors.toList()));
    return result;
  }

  @Override
  public long getCost() {
    return 0L;
  }

  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = new YTResultInternal(db);
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty(
        "steps",
        steps == null ? null
            : steps.stream().map(x -> x.serialize(db)).collect(Collectors.toList()));
    return result;
  }

  public void deserialize(YTResult serializedExecutionPlan) {
    List<YTResult> serializedSteps = serializedExecutionPlan.getProperty("steps");
    for (YTResult serializedStep : serializedSteps) {
      try {
        String className = serializedStep.getProperty(JAVA_TYPE);
        ExecutionStepInternal step =
            (ExecutionStepInternal) Class.forName(className).newInstance();
        step.deserialize(serializedStep);
        chain(step);
      } catch (Exception e) {
        throw YTException.wrapException(
            new YTCommandExecutionException("Cannot deserialize execution step:" + serializedStep),
            e);
      }
    }
  }

  @Override
  public OInternalExecutionPlan copy(CommandContext ctx) {
    OSelectExecutionPlan copy = new OSelectExecutionPlan(ctx);
    copyOn(copy, ctx);
    return copy;
  }

  protected void copyOn(OSelectExecutionPlan copy, CommandContext ctx) {
    ExecutionStep lastStep = null;
    for (ExecutionStep step : this.steps) {
      ExecutionStepInternal newStep =
          (ExecutionStepInternal) ((ExecutionStepInternal) step).copy(ctx);
      newStep.setPrevious((ExecutionStepInternal) lastStep);
      if (lastStep != null) {
        ((ExecutionStepInternal) lastStep).setNext(newStep);
      }
      lastStep = newStep;
      copy.getSteps().add(newStep);
    }
    copy.lastStep = copy.steps.size() == 0 ? null : copy.steps.get(copy.steps.size() - 1);
    copy.location = this.location;
    copy.statement = this.statement;
  }

  @Override
  public boolean canBeCached() {
    for (ExecutionStepInternal step : steps) {
      if (!step.canBeCached()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String getStatement() {
    return statement;
  }

  @Override
  public void setStatement(String statement) {
    this.statement = statement;
  }

  @Override
  public String getGenericStatement() {
    return this.genericStatement;
  }

  @Override
  public void setGenericStatement(String stm) {
    this.genericStatement = stm;
  }
}
