/* Generated By:JJTree: Do not edit this line. OLevelZeroIdentifier.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.AggregationContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class OLevelZeroIdentifier extends SimpleNode {

  protected OFunctionCall functionCall;
  protected Boolean self;
  protected OCollection collection;

  public OLevelZeroIdentifier(int id) {
    super(id);
  }

  public OLevelZeroIdentifier(OrientSql p, int id) {
    super(p, id);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (functionCall != null) {
      functionCall.toString(params, builder);
    } else if (Boolean.TRUE.equals(self)) {
      builder.append("@this");
    } else if (collection != null) {
      collection.toString(params, builder);
    }
  }

  public void toGenericStatement(StringBuilder builder) {
    if (functionCall != null) {
      functionCall.toGenericStatement(builder);
    } else if (Boolean.TRUE.equals(self)) {
      builder.append("@this");
    } else if (collection != null) {
      collection.toGenericStatement(builder);
    }
  }

  public Object execute(OIdentifiable iCurrentRecord, OCommandContext ctx) {
    if (functionCall != null) {
      return functionCall.execute(iCurrentRecord, ctx);
    }
    if (collection != null) {
      return collection.execute(iCurrentRecord, ctx);
    }
    if (Boolean.TRUE.equals(self)) {
      return iCurrentRecord;
    }
    throw new UnsupportedOperationException();
  }

  public Object execute(OResult iCurrentRecord, OCommandContext ctx) {
    if (functionCall != null) {
      return functionCall.execute(iCurrentRecord, ctx);
    }
    if (collection != null) {
      return collection.execute(iCurrentRecord, ctx);
    }
    if (Boolean.TRUE.equals(self)) {
      return iCurrentRecord;
    }
    throw new UnsupportedOperationException();
  }

  public boolean isIndexedFunctionCall() {
    if (functionCall != null) {
      return functionCall.isIndexedFunctionCall();
    }
    return false;
  }

  public boolean isFunctionAny() {
    if (functionCall != null) {
      return functionCall.getName().getStringValue().equalsIgnoreCase("any")
          && functionCall.params.size() == 0;
    }
    return false;
  }

  public boolean isFunctionAll() {
    if (functionCall != null) {
      return functionCall.getName().getStringValue().equalsIgnoreCase("all")
          && functionCall.params.size() == 0;
    }
    return false;
  }

  public long estimateIndexedFunction(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (functionCall != null) {
      return functionCall.estimateIndexedFunction(target, context, operator, right);
    }

    return -1;
  }

  public Iterable<OIdentifiable> executeIndexedFunction(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (functionCall != null) {
      return functionCall.executeIndexedFunction(target, context, operator, right);
    }
    return null;
  }

  /**
   * tests if current expression is an indexed funciton AND that function can also be executed
   * without using the index
   *
   * @param target   the query target
   * @param context  the execution context
   * @param operator
   * @param right
   * @return true if current expression is an indexed funciton AND that function can also be
   * executed without using the index, false otherwise
   */
  public boolean canExecuteIndexedFunctionWithoutIndex(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (this.functionCall == null) {
      return false;
    }
    return functionCall.canExecuteIndexedFunctionWithoutIndex(target, context, operator, right);
  }

  /**
   * tests if current expression is an indexed function AND that function can be used on this
   * target
   *
   * @param target   the query target
   * @param context  the execution context
   * @param operator
   * @param right
   * @return true if current expression involves an indexed function AND that function can be used
   * on this target, false otherwise
   */
  public boolean allowsIndexedFunctionExecutionOnTarget(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (this.functionCall == null) {
      return false;
    }
    return functionCall.allowsIndexedFunctionExecutionOnTarget(target, context, operator, right);
  }

  /**
   * tests if current expression is an indexed function AND the function has also to be executed
   * after the index search. In some cases, the index search is accurate, so this condition can be
   * excluded from further evaluation. In other cases the result from the index is a superset of the
   * expected result, so the function has to be executed anyway for further filtering
   *
   * @param target  the query target
   * @param context the execution context
   * @return true if current expression is an indexed function AND the function has also to be
   * executed after the index search.
   */
  public boolean executeIndexedFunctionAfterIndexSearch(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (this.functionCall == null) {
      return false;
    }
    return functionCall.executeIndexedFunctionAfterIndexSearch(target, context, operator, right);
  }

  public boolean isExpand() {
    if (functionCall != null) {
      return functionCall.isExpand();
    }
    return false;
  }

  public OExpression getExpandContent() {
    if (functionCall.getParams().size() != 1) {
      throw new OCommandExecutionException("Invalid expand expression: " + functionCall.toString());
    }
    return functionCall.getParams().get(0);
  }

  public boolean needsAliases(Set<String> aliases) {
    if (functionCall != null && functionCall.needsAliases(aliases)) {
      return true;
    }
    return collection != null && collection.needsAliases(aliases);
  }

  public boolean isAggregate() {
    if (functionCall != null && functionCall.isAggregate()) {
      return true;
    }
    return collection != null && collection.isAggregate();
  }

  public boolean isCount() {
    return functionCall != null && functionCall.name.getStringValue().equalsIgnoreCase("count");
  }

  public boolean isEarlyCalculated(OCommandContext ctx) {
    if (functionCall != null && functionCall.isEarlyCalculated(ctx)) {
      return true;
    }
    if (Boolean.TRUE.equals(self)) {
      return false;
    }
    return collection != null && collection.isEarlyCalculated(ctx);
  }

  public SimpleNode splitForAggregation(
      AggregateProjectionSplit aggregateProj, OCommandContext ctx) {
    if (isAggregate()) {
      OLevelZeroIdentifier result = new OLevelZeroIdentifier(-1);
      if (functionCall != null) {
        SimpleNode node = functionCall.splitForAggregation(aggregateProj, ctx);
        if (node instanceof OFunctionCall) {
          result.functionCall = (OFunctionCall) node;
        } else {
          return node;
        }
      } else if (collection != null) {
        result.collection = collection.splitForAggregation(aggregateProj, ctx);
        return result;
      } else {
        throw new IllegalStateException();
      }
      return result;
    } else {
      return this;
    }
  }

  public AggregationContext getAggregationContext(OCommandContext ctx) {
    if (isAggregate()) {
      if (functionCall != null) {
        return functionCall.getAggregationContext(ctx);
      }
    }
    throw new OCommandExecutionException("cannot aggregate on " + this);
  }

  public OLevelZeroIdentifier copy() {
    OLevelZeroIdentifier result = new OLevelZeroIdentifier(-1);
    result.functionCall = functionCall == null ? null : functionCall.copy();
    result.self = self;
    result.collection = collection == null ? null : collection.copy();
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

    OLevelZeroIdentifier that = (OLevelZeroIdentifier) o;

    if (!Objects.equals(functionCall, that.functionCall)) {
      return false;
    }
    if (!Objects.equals(self, that.self)) {
      return false;
    }
    return Objects.equals(collection, that.collection);
  }

  @Override
  public int hashCode() {
    int result = functionCall != null ? functionCall.hashCode() : 0;
    result = 31 * result + (self != null ? self.hashCode() : 0);
    result = 31 * result + (collection != null ? collection.hashCode() : 0);
    return result;
  }

  public void setCollection(OCollection collection) {
    this.collection = collection;
  }

  public boolean refersToParent() {
    if (functionCall != null && functionCall.refersToParent()) {
      return true;
    }
    return collection != null && collection.refersToParent();
  }

  public OFunctionCall getFunctionCall() {
    return functionCall;
  }

  public Boolean getSelf() {
    return self;
  }

  public OCollection getCollection() {
    return collection;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    if (functionCall != null) {
      result.setProperty("functionCall", functionCall.serialize());
    }
    result.setProperty("self", self);
    if (collection != null) {
      result.setProperty("collection", collection.serialize());
    }
    return result;
  }

  public void deserialize(OResult fromResult) {
    if (fromResult.getProperty("functionCall") != null) {
      functionCall = new OFunctionCall(-1);
      functionCall.deserialize(fromResult.getProperty("functionCall"));
    }
    self = fromResult.getProperty("self");
    if (fromResult.getProperty("collection") != null) {
      collection = new OCollection(-1);
      collection.deserialize(fromResult.getProperty("collection"));
    }
  }

  public void extractSubQueries(OIdentifier letAlias, SubQueryCollector collector) {
    if (this.functionCall != null) {
      this.functionCall.extractSubQueries(letAlias, collector);
    }
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (this.functionCall != null) {
      this.functionCall.extractSubQueries(collector);
    }
  }

  public boolean isCacheable() {
    if (functionCall != null) {
      return functionCall.isCacheable();
    }
    if (collection != null) {
      return collection.isCacheable();
    }
    return false;
  }
}
/* JavaCC - OriginalChecksum=0305fcf120ba9395b4c975f85cdade72 (do not edit this line) */
