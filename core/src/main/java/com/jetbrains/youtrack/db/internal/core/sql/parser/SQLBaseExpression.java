/* Generated By:JJTree: Do not edit this line. SQLBaseExpression.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.sql.executor.AggregationContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.MetadataPath;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class SQLBaseExpression extends SQLMathExpression {

  protected SQLNumber number;

  private SQLBaseIdentifier identifier;

  protected SQLInputParameter inputParam;

  protected String string;

  SQLModifier modifier;

  public SQLBaseExpression(int id) {
    super(id);
  }

  public SQLBaseExpression(YouTrackDBSql p, int id) {
    super(p, id);
  }

  public SQLBaseExpression(SQLIdentifier identifier) {
    super(-1);
    this.identifier = new SQLBaseIdentifier(identifier);
  }

  public SQLBaseExpression(String string) {
    super(-1);
    this.string = "\"" + StringSerializerHelper.encode(string) + "\"";
  }

  public SQLBaseExpression(SQLIdentifier identifier, SQLModifier modifier) {
    this(identifier);
    if (modifier != null) {
      this.modifier = modifier;
    }
  }

  public SQLBaseExpression(SQLRecordAttribute attr, SQLModifier modifier) {
    super(-1);
    this.identifier = new SQLBaseIdentifier(attr);
    if (modifier != null) {
      this.modifier = modifier;
    }
  }

  @Override
  public String toString() {
    return super.toString();
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (number != null) {
      number.toString(params, builder);
    } else if (identifier != null) {
      identifier.toString(params, builder);
    } else if (string != null) {
      builder.append(string);
    } else if (inputParam != null) {
      inputParam.toString(params, builder);
    }

    if (modifier != null) {
      modifier.toString(params, builder);
    }
  }

  public void toGenericStatement(StringBuilder builder) {
    if (number != null) {
      number.toGenericStatement(builder);
    } else if (identifier != null) {
      identifier.toGenericStatement(builder);
    } else if (string != null) {
      builder.append(PARAMETER_PLACEHOLDER);
    } else if (inputParam != null) {
      inputParam.toGenericStatement(builder);
    }
    if (modifier != null) {
      modifier.toGenericStatement(builder);
    }
  }

  public Object execute(Identifiable iCurrentRecord, CommandContext ctx) {
    Object result = null;
    if (number != null) {
      result = number.getValue();
    } else if (identifier != null) {
      result = identifier.execute(iCurrentRecord, ctx);
    } else if (string != null && string.length() > 1) {
      result = StringSerializerHelper.decode(string.substring(1, string.length() - 1));
    } else if (inputParam != null) {
      result = inputParam.getValue(ctx.getInputParameters());
    }

    if (modifier != null) {
      result = modifier.execute(iCurrentRecord, result, ctx);
    }

    return result;
  }

  public Object execute(Result iCurrentRecord, CommandContext ctx) {
    Object result = null;
    if (number != null) {
      result = number.getValue();
    } else if (identifier != null) {
      result = identifier.execute(iCurrentRecord, ctx);
    } else if (string != null && string.length() > 1) {
      result = StringSerializerHelper.decode(string.substring(1, string.length() - 1));
    } else if (inputParam != null) {
      result = inputParam.getValue(ctx.getInputParameters());
    }
    if (modifier != null) {
      result = modifier.execute(iCurrentRecord, result, ctx);
    }
    return result;
  }

  @Override
  protected boolean supportsBasicCalculation() {
    return true;
  }

  @Override
  public boolean isFunctionAny() {
    if (this.identifier == null) {
      return false;
    }
    return identifier.isFunctionAny();
  }

  @Override
  public boolean isFunctionAll() {
    if (this.identifier == null) {
      return false;
    }
    return identifier.isFunctionAll();
  }

  @Override
  public boolean isIndexedFunctionCall(DatabaseSessionInternal session) {
    if (this.identifier == null) {
      return false;
    }
    return identifier.isIndexedFunctionCall(session);
  }

  public long estimateIndexedFunction(
      SQLFromClause target, CommandContext context, SQLBinaryCompareOperator operator,
      Object right) {
    if (this.identifier == null) {
      return -1;
    }
    return identifier.estimateIndexedFunction(target, context, operator, right);
  }

  public Iterable<Identifiable> executeIndexedFunction(
      SQLFromClause target, CommandContext context, SQLBinaryCompareOperator operator,
      Object right) {
    if (this.identifier == null) {
      return null;
    }
    return identifier.executeIndexedFunction(target, context, operator, right);
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
      SQLFromClause target, CommandContext context, SQLBinaryCompareOperator operator,
      Object right) {
    if (this.identifier == null) {
      return false;
    }
    return identifier.canExecuteIndexedFunctionWithoutIndex(target, context, operator, right);
  }

  /**
   * tests if current expression is an indexed function AND that function can be used on this
   * target
   *
   * @param target   the query target
   * @param context  the execution context
   * @param operator
   * @param right
   * @return true if current expression is an indexed function AND that function can be used on this
   * target, false otherwise
   */
  public boolean allowsIndexedFunctionExecutionOnTarget(
      SQLFromClause target, CommandContext context, SQLBinaryCompareOperator operator,
      Object right) {
    if (this.identifier == null) {
      return false;
    }
    return identifier.allowsIndexedFunctionExecutionOnTarget(target, context, operator, right);
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
      SQLFromClause target, CommandContext context, SQLBinaryCompareOperator operator,
      Object right) {
    if (this.identifier == null) {
      return false;
    }
    return identifier.executeIndexedFunctionAfterIndexSearch(target, context, operator, right);
  }

  @Override
  public boolean isBaseIdentifier() {
    return identifier != null && modifier == null && identifier.isBaseIdentifier();
  }

  public Optional<MetadataPath> getPath() {
    if (identifier != null && identifier.isBaseIdentifier()) {
      if (modifier != null) {
        var path = modifier.getPath();
        if (path.isPresent()) {
          path.get().addPre(this.identifier.getSuffix().identifier.getStringValue());
          return path;
        } else {
          return Optional.empty();
        }
      } else {
        return Optional.of(
            new MetadataPath(this.identifier.getSuffix().identifier.getStringValue()));
      }
    }
    return Optional.empty();
  }

  @Override
  public Collate getCollate(Result currentRecord, CommandContext ctx) {
    return identifier != null && modifier == null
        ? identifier.getCollate(currentRecord, ctx)
        : null;
  }

  public boolean isEarlyCalculated(CommandContext ctx) {
    if (number != null || inputParam != null || string != null) {
      return true;
    }
    return identifier != null && identifier.isEarlyCalculated(ctx);
  }

  @Override
  public boolean isExpand() {
    if (identifier != null) {
      return identifier.isExpand();
    }
    return false;
  }

  @Override
  public SQLExpression getExpandContent() {
    return this.identifier.getExpandContent();
  }

  public boolean needsAliases(Set<String> aliases) {
    if (this.identifier != null && this.identifier.needsAliases(aliases)) {
      return true;
    }
    return modifier != null && modifier.needsAliases(aliases);
  }

  @Override
  public boolean isAggregate(DatabaseSessionInternal session) {
    return identifier != null && identifier.isAggregate(session);
  }

  @Override
  public boolean isCount() {
    return identifier != null && identifier.isCount();
  }

  public SimpleNode splitForAggregation(
      AggregateProjectionSplit aggregateProj, CommandContext ctx) {
    if (isAggregate(ctx.getDatabaseSession())) {
      var splitResult = identifier.splitForAggregation(aggregateProj, ctx);
      if (splitResult instanceof SQLBaseIdentifier) {
        var result = new SQLBaseExpression(-1);
        result.identifier = (SQLBaseIdentifier) splitResult;
        return result;
      }
      return splitResult;
    } else {
      return this;
    }
  }

  public AggregationContext getAggregationContext(CommandContext ctx) {
    if (identifier != null) {
      return identifier.getAggregationContext(ctx);
    } else {
      throw new CommandExecutionException(ctx.getDatabaseSession(), "cannot aggregate on " + this);
    }
  }

  @Override
  public SQLBaseExpression copy() {
    var result = new SQLBaseExpression(-1);
    result.number = number == null ? null : number.copy();
    result.identifier = identifier == null ? null : identifier.copy();
    result.inputParam = inputParam == null ? null : inputParam.copy();
    result.string = string;
    result.modifier = modifier == null ? null : modifier.copy();
    return result;
  }

  public boolean refersToParent() {
    if (identifier != null && identifier.refersToParent()) {
      return true;
    }
    return modifier != null && modifier.refersToParent();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    var that = (SQLBaseExpression) o;

    if (!Objects.equals(number, that.number)) {
      return false;
    }
    if (!Objects.equals(identifier, that.identifier)) {
      return false;
    }
    if (!Objects.equals(inputParam, that.inputParam)) {
      return false;
    }
    if (!Objects.equals(string, that.string)) {
      return false;
    }
    return Objects.equals(modifier, that.modifier);
  }

  @Override
  public int hashCode() {
    var result = number != null ? number.hashCode() : 0;
    result = 31 * result + (identifier != null ? identifier.hashCode() : 0);
    result = 31 * result + (inputParam != null ? inputParam.hashCode() : 0);
    result = 31 * result + (string != null ? string.hashCode() : 0);
    result = 31 * result + (modifier != null ? modifier.hashCode() : 0);
    return result;
  }

  public void setIdentifier(SQLBaseIdentifier identifier) {
    this.identifier = identifier;
  }

  public SQLBaseIdentifier getIdentifier() {
    return identifier;
  }

  public SQLModifier getModifier() {
    return modifier;
  }

  public List<String> getMatchPatternInvolvedAliases() {
    if (this.identifier != null && this.identifier.toString().equals("$matched")) {
      if (modifier != null && modifier.suffix != null && modifier.suffix.getIdentifier() != null) {
        return Collections.singletonList(modifier.suffix.getIdentifier().toString());
      }
    }
    return Collections.emptyList();
  }

  @Override
  public void applyRemove(ResultInternal result, CommandContext ctx) {
    if (identifier != null) {
      if (modifier == null) {
        identifier.applyRemove(result, ctx);
      } else {
        var val = identifier.execute(result, ctx);
        modifier.applyRemove(val, result, ctx);
      }
    }
  }

  public Result serialize(DatabaseSessionInternal db) {
    var result = (ResultInternal) super.serialize(db);

    if (number != null) {
      result.setProperty("number", number.serialize(db));
    }
    if (identifier != null) {
      result.setProperty("identifier", identifier.serialize(db));
    }
    if (inputParam != null) {
      result.setProperty("inputParam", inputParam.serialize(db));
    }
    if (string != null) {
      result.setProperty("string", string);
    }
    if (modifier != null) {
      result.setProperty("modifier", modifier.serialize(db));
    }
    return result;
  }

  public void deserialize(Result fromResult) {
    super.deserialize(fromResult);

    if (fromResult.getProperty("number") != null) {
      number = new SQLNumber(-1);
      number.deserialize(fromResult.getProperty("number"));
    }
    if (fromResult.getProperty("identifier") != null) {
      identifier = new SQLBaseIdentifier(-1);
      identifier.deserialize(fromResult.getProperty("identifier"));
    }
    if (fromResult.getProperty("inputParam") != null) {
      inputParam = SQLInputParameter.deserializeFromOResult(fromResult.getProperty("inputParam"));
    }

    if (fromResult.getProperty("string") != null) {
      string = fromResult.getProperty("string");
    }
    if (fromResult.getProperty("modifier") != null) {
      modifier = new SQLModifier(-1);
      modifier.deserialize(fromResult.getProperty("modifier"));
    }
  }

  @Override
  public boolean isDefinedFor(Result currentRecord) {
    if (this.identifier != null) {
      if (modifier == null) {
        return identifier.isDefinedFor(currentRecord);
      }
    }
    return true;
  }

  @Override
  public boolean isDefinedFor(DatabaseSessionInternal db, Entity currentRecord) {
    if (this.identifier != null) {
      if (modifier == null) {
        return identifier.isDefinedFor(db, currentRecord);
      }
    }
    return true;
  }

  public void extractSubQueries(SQLIdentifier letAlias, SubQueryCollector collector) {
    if (this.identifier != null) {
      this.identifier.extractSubQueries(letAlias, collector);
    }
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (this.identifier != null) {
      this.identifier.extractSubQueries(collector);
    }
  }

  public boolean isCacheable(DatabaseSessionInternal session) {
    if (modifier != null && !modifier.isCacheable(session)) {
      return false;
    }
    if (identifier != null) {
      return identifier.isCacheable(session);
    }

    return true;
  }

  public void setInputParam(SQLInputParameter inputParam) {
    this.inputParam = inputParam;
  }

  public boolean isIndexChain(CommandContext ctx, SchemaClassInternal clazz) {
    if (modifier == null) {
      return false;
    }
    var db = ctx.getDatabaseSession();
    if (identifier.isIndexChain(ctx, clazz)) {
      var prop = clazz.getProperty(db,
          identifier.getSuffix().getIdentifier().getStringValue());
      var linkedClass = (SchemaClassInternal) prop.getLinkedClass(ctx.getDatabaseSession());
      if (linkedClass != null) {
        return modifier.isIndexChain(ctx, linkedClass);
      }
    }
    return false;
  }
}

/* JavaCC - OriginalChecksum=71b3e2d1b65c923dc7cfe11f9f449d2b (do not edit this line) */
