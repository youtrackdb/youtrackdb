/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.DocumentHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionStep;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Basic implementation of CommandContext interface that stores variables in a map. Supports
 * parent/child context to build a tree of contexts. If a variable is not found on current object
 * the search is applied recursively on child contexts.
 */
public class BasicCommandContext implements CommandContext {

  public static final String EXECUTION_BEGUN = "EXECUTION_BEGUN";
  public static final String TIMEOUT_MS = "TIMEOUT_MS";
  public static final String TIMEOUT_STRATEGY = "TIMEOUT_STARTEGY";
  public static final String INVALID_COMPARE_COUNT = "INVALID_COMPARE_COUNT";

  protected DatabaseSessionInternal database;
  protected Object[] args;

  protected boolean recordMetrics = false;
  protected CommandContext parent;
  protected CommandContext child;
  protected Map<String, Object> variables;

  protected Map<Object, Object> inputParameters;

  protected Set<String> declaredScriptVariables = new HashSet<>();

  // MANAGES THE TIMEOUT
  private long executionStartedOn;
  private long timeoutMs;
  private CommandContext.TIMEOUT_STRATEGY
      timeoutStrategy;
  protected AtomicLong resultsProcessed = new AtomicLong(0);
  protected Set<Object> uniqueResult = new HashSet<Object>();
  private final Map<ExecutionStep, StepStats> stepStats = new IdentityHashMap<>();
  private final LinkedList<StepStats> currentStepStats = new LinkedList<>();

  public BasicCommandContext() {
  }

  public BasicCommandContext(DatabaseSessionInternal session) {
    this.database = session;
  }

  public Object getVariable(String iName) {
    return getVariable(iName, null);
  }

  public Object getVariable(String iName, final Object iDefault) {
    if (iName == null) {
      return iDefault;
    }

    Object result = null;

    if (iName.startsWith("$")) {
      iName = iName.substring(1);
    }

    int pos = StringSerializerHelper.getLowerIndexOf(iName, 0, ".", "[");

    String firstPart;
    String lastPart;
    if (pos > -1) {
      firstPart = iName.substring(0, pos);
      if (iName.charAt(pos) == '.') {
        pos++;
      }
      lastPart = iName.substring(pos);
      if (firstPart.equalsIgnoreCase("PARENT") && parent != null) {
        // UP TO THE PARENT
        if (lastPart.startsWith("$")) {
          result = parent.getVariable(lastPart.substring(1));
        } else {
          result = DocumentHelper.getFieldValue(getDatabase(), parent, lastPart);
        }

        return result != null ? resolveValue(result) : iDefault;

      } else if (firstPart.equalsIgnoreCase("ROOT")) {
        CommandContext p = this;
        while (p.getParent() != null) {
          p = p.getParent();
        }

        if (lastPart.startsWith("$")) {
          result = p.getVariable(lastPart.substring(1));
        } else {
          result = DocumentHelper.getFieldValue(getDatabase(), p, lastPart, this);
        }

        return result != null ? resolveValue(result) : iDefault;
      }
    } else {
      firstPart = iName;
      lastPart = null;
    }

    if (firstPart.equalsIgnoreCase("CONTEXT")) {
      result = getVariables();
    } else if (firstPart.equalsIgnoreCase("PARENT")) {
      result = parent;
    } else if (firstPart.equalsIgnoreCase("ROOT")) {
      CommandContext p = this;
      while (p.getParent() != null) {
        p = p.getParent();
      }
      result = p;
    } else {
      if (variables != null && variables.containsKey(firstPart)) {
        result = variables.get(firstPart);
      } else {
        if (child != null) {
          result = child.getVariable(firstPart);
        } else {
          result = getVariableFromParentHierarchy(firstPart);
        }
      }
    }

    if (pos > -1) {
      result = DocumentHelper.getFieldValue(getDatabase(), result, lastPart, this);
    }

    return result != null ? resolveValue(result) : iDefault;
  }

  private Object resolveValue(Object value) {
    if (value instanceof DynamicVariable) {
      value = ((DynamicVariable) value).resolve(this);
    }
    return value;
  }

  protected Object getVariableFromParentHierarchy(String varName) {
    if (this.variables != null && variables.containsKey(varName)) {
      return variables.get(varName);
    }
    if (parent != null && parent instanceof BasicCommandContext) {
      return ((BasicCommandContext) parent).getVariableFromParentHierarchy(varName);
    }
    return null;
  }

  public CommandContext setDynamicVariable(String iName, final DynamicVariable iValue) {
    return setVariable(iName, iValue);
  }

  public CommandContext setVariable(String iName, final Object iValue) {
    if (iName == null) {
      return null;
    }

    if (iName.startsWith("$")) {
      iName = iName.substring(1);
    }

    init();

    int pos = StringSerializerHelper.getHigherIndexOf(iName, 0, ".", "[");
    if (pos > -1) {
      Object nested = getVariable(iName.substring(0, pos));
      if (nested != null && nested instanceof CommandContext) {
        ((CommandContext) nested).setVariable(iName.substring(pos + 1), iValue);
      }
    } else {
      if (variables.containsKey(iName)) {
        variables.put(
            iName, iValue); // this is a local existing variable, so it's bound to current contex
      } else if (parent != null
          && parent instanceof BasicCommandContext
          && ((BasicCommandContext) parent).hasVariable(iName)) {
        if ("current".equalsIgnoreCase(iName) || "parent".equalsIgnoreCase(iName)) {
          variables.put(iName, iValue);
        } else {
          parent.setVariable(
              iName,
              iValue); // it is an existing variable in parent context, so it's bound to parent
          // context
        }
      } else {
        variables.put(iName, iValue); // it's a new variable, so it's created in this context
      }
    }
    return this;
  }

  boolean hasVariable(String iName) {
    if (variables != null && variables.containsKey(iName)) {
      return true;
    }
    if (parent != null && parent instanceof BasicCommandContext) {
      return ((BasicCommandContext) parent).hasVariable(iName);
    }
    return false;
  }

  @Override
  public CommandContext incrementVariable(String iName) {
    if (iName != null) {
      if (iName.startsWith("$")) {
        iName = iName.substring(1);
      }

      init();

      int pos = StringSerializerHelper.getHigherIndexOf(iName, 0, ".", "[");
      if (pos > -1) {
        Object nested = getVariable(iName.substring(0, pos));
        if (nested != null && nested instanceof CommandContext) {
          ((CommandContext) nested).incrementVariable(iName.substring(pos + 1));
        }
      } else {
        final Object v = variables.get(iName);
        if (v == null) {
          variables.put(iName, 1);
        } else if (v instanceof Number) {
          variables.put(iName, PropertyType.increment((Number) v, 1));
        } else {
          throw new IllegalArgumentException(
              "Variable '" + iName + "' is not a number, but: " + v.getClass());
        }
      }
    }
    return this;
  }

  public long updateMetric(final String iName, final long iValue) {
    if (!recordMetrics) {
      return -1;
    }

    init();
    Long value = (Long) variables.get(iName);
    if (value == null) {
      value = iValue;
    } else {
      value = Long.valueOf(value.longValue() + iValue);
    }
    variables.put(iName, value);
    return value.longValue();
  }

  /**
   * Returns a read-only map with all the variables.
   */
  public Map<String, Object> getVariables() {
    final HashMap<String, Object> map = new HashMap<String, Object>();
    if (child != null) {
      map.putAll(child.getVariables());
    }

    if (variables != null) {
      map.putAll(variables);
    }

    return map;
  }

  /**
   * Set the inherited context avoiding to copy all the values every time.
   *
   * @return
   */
  public CommandContext setChild(final CommandContext iContext) {
    if (iContext == null) {
      if (child != null) {
        // REMOVE IT
        child.setParent(null);
        child.setDatabase(null);

        child = null;
      }
    } else if (child != iContext) {
      // ADD IT
      child = iContext;
      iContext.setParent(this);
      child.setDatabase(database);
    }

    return this;
  }

  public CommandContext getParent() {
    return parent;
  }

  public CommandContext setParent(final CommandContext iParentContext) {
    if (parent != iParentContext) {
      parent = iParentContext;
      if (parent != null) {
        parent.setChild(this);
      }
    }
    return this;
  }

  public CommandContext setParentWithoutOverridingChild(final CommandContext iParentContext) {
    if (parent != iParentContext) {
      parent = iParentContext;
    }
    return this;
  }

  @Override
  public String toString() {
    return getVariables().toString();
  }

  public boolean isRecordingMetrics() {
    return recordMetrics;
  }

  public CommandContext setRecordingMetrics(final boolean recordMetrics) {
    this.recordMetrics = recordMetrics;
    return this;
  }

  @Override
  public void beginExecution(final long iTimeout, final TIMEOUT_STRATEGY iStrategy) {
    if (iTimeout > 0) {
      executionStartedOn = System.currentTimeMillis();
      timeoutMs = iTimeout;
      timeoutStrategy = iStrategy;
    }
  }

  public boolean checkTimeout() {
    if (timeoutMs > 0) {
      if (System.currentTimeMillis() - executionStartedOn > timeoutMs) {
        // TIMEOUT!
        switch (timeoutStrategy) {
          case RETURN:
            return false;
          case EXCEPTION:
            throw new TimeoutException("Command execution timeout exceed (" + timeoutMs + "ms)");
        }
      }
    } else if (parent != null)
    // CHECK THE TIMER OF PARENT CONTEXT
    {
      return parent.checkTimeout();
    }

    return true;
  }

  @Override
  public CommandContext copy() {
    final BasicCommandContext copy = new BasicCommandContext();
    copy.init();

    if (variables != null && !variables.isEmpty()) {
      copy.variables.putAll(variables);
    }

    copy.recordMetrics = recordMetrics;

    copy.child = child.copy();
    copy.child.setParent(copy);

    copy.setDatabase(null);

    return copy;
  }

  @Override
  public void merge(final CommandContext iContext) {
    // TODO: SOME VALUES NEED TO BE MERGED
  }

  private void init() {
    if (variables == null) {
      variables = new HashMap<String, Object>();
    }
  }

  public Map<Object, Object> getInputParameters() {
    if (inputParameters != null) {
      return inputParameters;
    }

    return parent == null ? null : parent.getInputParameters();
  }

  public void setInputParameters(Map<Object, Object> inputParameters) {
    this.inputParameters = inputParameters;
  }

  /**
   * returns the number of results processed. This is intended to be used with LIMIT in SQL
   * statements
   *
   * @return
   */
  public AtomicLong getResultsProcessed() {
    return resultsProcessed;
  }

  /**
   * adds an item to the unique result set
   *
   * @param o the result item to add
   * @return true if the element is successfully added (it was not present yet), false otherwise (it
   * was already present)
   */
  public synchronized boolean addToUniqueResult(Object o) {
    Object toAdd = o;
    if (o instanceof EntityImpl && ((EntityImpl) o).getIdentity().isNew()) {
      toAdd = new DocumentEqualityWrapper((EntityImpl) o);
    }
    return this.uniqueResult.add(toAdd);
  }

  public DatabaseSessionInternal getDatabase() {
    if (database != null) {
      return database;
    }

    if (parent != null) {
      database = parent.getDatabase();
    }

    if (database == null && !(this instanceof ServerCommandContext)) {
      throw new DatabaseException("No database found in SQL context");
    }

    return database;
  }


  public void setDatabase(DatabaseSessionInternal database) {
    this.database = database;

    if (child != null) {
      child.setDatabase(database);
    }
  }

  @Override
  public void declareScriptVariable(String varName) {
    this.declaredScriptVariables.add(varName);
  }

  @Override
  public boolean isScriptVariableDeclared(String varName) {
    if (varName == null || varName.length() == 0) {
      return false;
    }
    String dollarVar = varName;
    if (!dollarVar.startsWith("$")) {
      dollarVar = "$" + varName;
    }
    varName = dollarVar.substring(1);
    if (variables != null && (variables.containsKey(varName) || variables.containsKey(dollarVar))) {
      return true;
    }
    return declaredScriptVariables.contains(varName)
        || declaredScriptVariables.contains(dollarVar)
        || (parent != null && parent.isScriptVariableDeclared(varName));
  }

  public void startProfiling(ExecutionStep step) {
    StepStats stats = stepStats.get(step);
    if (stats == null) {
      stats = new StepStats();
      stepStats.put(step, stats);
    }
    if (!this.currentStepStats.isEmpty()) {
      this.currentStepStats.getLast().pause();
    }
    stats.start();
    this.currentStepStats.push(stats);
  }

  public void endProfiling(ExecutionStep step) {
    if (!this.currentStepStats.isEmpty()) {
      this.currentStepStats.pop().end();
      if (!this.currentStepStats.isEmpty()) {
        this.currentStepStats.getLast().resume();
      }
    }
  }

  @Override
  public StepStats getStats(ExecutionStep step) {
    return stepStats.get(step);
  }
}
