package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultInternal;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class OJsonItem {

  protected OIdentifier leftIdentifier;
  protected String leftString;
  protected OExpression right;

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (leftIdentifier != null) {
      builder.append("\"");
      leftIdentifier.toString(params, builder);
      builder.append("\"");
    }
    if (leftString != null) {
      builder.append("\"");
      builder.append(OExpression.encode(leftString));
      builder.append("\"");
    }
    builder.append(": ");
    right.toString(params, builder);
  }

  public void toGenericStatement(StringBuilder builder) {
    if (leftIdentifier != null) {
      builder.append("\"");
      leftIdentifier.toGenericStatement(builder);
      builder.append("\"");
    }
    if (leftString != null) {
      builder.append("\"");
      builder.append(OExpression.encode(leftString));
      builder.append("\"");
    }
    builder.append(": ");
    right.toGenericStatement(builder);
  }

  public String getLeftValue() {
    if (leftString != null) {
      return leftString;
    }
    if (leftIdentifier != null) {
      return leftIdentifier.getStringValue();
    }
    return null;
  }

  public boolean needsAliases(Set<String> aliases) {
    if (aliases.contains(leftIdentifier.getStringValue())) {
      return true;
    }
    return right.needsAliases(aliases);
  }

  public boolean isAggregate(YTDatabaseSessionInternal session) {
    return right.isAggregate(session);
  }

  public OJsonItem splitForAggregation(
      AggregateProjectionSplit aggregateSplit, OCommandContext ctx) {
    if (isAggregate(ctx.getDatabase())) {
      OJsonItem item = new OJsonItem();
      item.leftIdentifier = leftIdentifier;
      item.leftString = leftString;
      item.right = right.splitForAggregation(aggregateSplit, ctx);
      return item;
    } else {
      return this;
    }
  }

  public OJsonItem copy() {
    OJsonItem result = new OJsonItem();
    result.leftIdentifier = leftIdentifier == null ? null : leftIdentifier.copy();
    result.leftString = leftString;
    result.right = right.copy();
    return result;
  }

  public void extractSubQueries(SubQueryCollector collector) {
    right.extractSubQueries(collector);
  }

  public boolean refersToParent() {
    return right != null && right.refersToParent();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OJsonItem oJsonItem = (OJsonItem) o;

    if (!Objects.equals(leftIdentifier, oJsonItem.leftIdentifier)) {
      return false;
    }
    if (!Objects.equals(leftString, oJsonItem.leftString)) {
      return false;
    }
    return Objects.equals(right, oJsonItem.right);
  }

  @Override
  public int hashCode() {
    int result = leftIdentifier != null ? leftIdentifier.hashCode() : 0;
    result = 31 * result + (leftString != null ? leftString.hashCode() : 0);
    result = 31 * result + (right != null ? right.hashCode() : 0);
    return result;
  }

  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = new YTResultInternal(db);
    result.setProperty("leftIdentifier", leftIdentifier.serialize(db));
    result.setProperty("leftString", leftString);
    result.setProperty("right", right.serialize(db));
    return result;
  }

  public void deserialize(YTResult fromResult) {
    if (fromResult.getProperty("leftIdentifier") != null) {
      leftIdentifier = OIdentifier.deserialize(fromResult.getProperty("leftIdentifier"));
    }
    if (fromResult.getProperty("leftString") != null) {
      leftString = fromResult.getProperty("leftString");
    }
    if (fromResult.getProperty("right") != null) {
      right = new OExpression(-1);
      right.deserialize(fromResult.getProperty("right"));
    }
  }
}
