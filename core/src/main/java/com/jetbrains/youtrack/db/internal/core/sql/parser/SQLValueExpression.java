/* Generated By:JJTree: Do not edit this line. SQLExpression.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.internal.core.sql.executor.AggregationContext;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * this class is only used by the query executor to store pre-calculated values and store them in a
 * temporary AST. It's not produced by parsing
 */
public class SQLValueExpression extends SQLExpression {

  public SQLValueExpression(Object val) {
    super(-1);
    this.value = val;
  }

  public Object execute(Identifiable iCurrentRecord, CommandContext ctx) {
    return value;
  }

  public Object execute(Result iCurrentRecord, CommandContext ctx) {
    return value;
  }

  public boolean isBaseIdentifier() {
    return false;
  }

  public boolean isEarlyCalculated() {
    return true;
  }

  public SQLIdentifier getDefaultAlias() {
    return new SQLIdentifier(String.valueOf(value));
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append(value);
  }

  public boolean supportsBasicCalculation() {
    return true;
  }

  public boolean isIndexedFunctionCal(DatabaseSessionInternal session) {
    return false;
  }

  public boolean canExecuteIndexedFunctionWithoutIndex(
      SQLFromClause target, CommandContext context, SQLBinaryCompareOperator operator,
      Object right) {
    return false;
  }

  public boolean allowsIndexedFunctionExecutionOnTarget(
      SQLFromClause target, CommandContext context, SQLBinaryCompareOperator operator,
      Object right) {
    return false;
  }

  public boolean executeIndexedFunctionAfterIndexSearch(
      SQLFromClause target, CommandContext context, SQLBinaryCompareOperator operator,
      Object right) {
    return false;
  }

  public boolean isExpand() {
    return false;
  }

  public SQLValueExpression getExpandContent() {
    return null;
  }

  public boolean needsAliases(Set<String> aliases) {
    return false;
  }

  public boolean isAggregate(DatabaseSessionInternal session) {
    return false;
  }

  public SQLValueExpression splitForAggregation(AggregateProjectionSplit aggregateSplit) {
    return this;
  }

  public AggregationContext getAggregationContext(CommandContext ctx) {
    throw new CommandExecutionException("Cannot aggregate on " + this);
  }

  public SQLValueExpression copy() {

    SQLValueExpression result = new SQLValueExpression(-1);
    result.value = value;
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

    SQLValueExpression that = (SQLValueExpression) o;
    return that.value == this.value;
  }

  @Override
  public int hashCode() {
    return 1;
  }

  public void extractSubQueries(SubQueryCollector collector) {
  }

  public void extractSubQueries(SQLIdentifier letAlias, SubQueryCollector collector) {
  }

  public boolean refersToParent() {

    return false;
  }

  List<String> getMatchPatternInvolvedAliases() {
    return null;
  }

  public void applyRemove(ResultInternal result, CommandContext ctx) {
    throw new CommandExecutionException("Cannot apply REMOVE " + this);
  }

  public boolean isCount() {
    return false;
  }

  public Result serialize(DatabaseSessionInternal db) {
    throw new UnsupportedOperationException(
        "Cannot serialize value expression (not supported yet)");
  }

  public void deserialize(Result fromResult) {
    throw new UnsupportedOperationException(
        "Cannot deserialize value expression (not supported yet)");
  }

  public boolean isDefinedFor(Result currentRecord) {
    return true;
  }

  public boolean isDefinedFor(Entity currentRecord) {
    return true;
  }

  public Collate getCollate(Result currentRecord, CommandContext ctx) {
    return null;
  }

  public boolean isCacheable(DatabaseSessionInternal session) {
    return true;
  }
}
/* JavaCC - OriginalChecksum=9c860224b121acdc89522ae97010be01 (do not edit this line) */
