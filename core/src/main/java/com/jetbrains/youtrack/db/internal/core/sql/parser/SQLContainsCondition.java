/* Generated By:JJTree: Do not edit this line. SQLContainsCondition.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.operator.QueryOperatorEquals;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SQLContainsCondition extends SQLBooleanExpression {

  protected SQLExpression left;
  protected SQLExpression right;
  protected SQLBooleanExpression condition;

  public SQLContainsCondition(int id) {
    super(id);
  }

  public SQLContainsCondition(YouTrackDBSql p, int id) {
    super(p, id);
  }

  public boolean execute(DatabaseSessionInternal session, Object left, Object right) {
    if (left instanceof Collection) {
      if (right instanceof Collection) {
        if (((Collection) right).size() == 1) {
          var item = ((Collection) right).iterator().next();
          if (item instanceof Result && ((Result) item).getPropertyNames().size() == 1) {
            var propValue =
                ((Result) item).getProperty(
                    ((Result) item).getPropertyNames().iterator().next());
            if (((Collection) left).contains(propValue)) {
              return true;
            }
          }
          if (((Collection) left).contains(item)) {
            return true;
          }
          if (item instanceof Result) {
            item = ((Result) item).asEntity();
          }
          if (item instanceof Identifiable && ((Collection) left).contains(item)) {
            return true;
          }
        }

        return MultiValue.contains(left, right);
      }
      if (right instanceof Iterable) {
        right = ((Iterable) right).iterator();
      }
      if (right instanceof Iterator iterator) {
        while (iterator.hasNext()) {
          var next = iterator.next();
          if (!((Collection) left).contains(next)) {
            return false;
          }
        }
      }
      for (var o : (Collection) left) {
        if (equalsInContainsSpace(session, o, right)) {
          return true;
        }
      }
      return false;
    }

    Iterator leftIterator = null;
    if (left instanceof Iterable) {
      leftIterator = ((Iterable) left).iterator();
    } else if (left instanceof Iterator) {
      leftIterator = (Iterator) left;
    }
    if (leftIterator != null) {
      if (!(right instanceof Iterable)) {
        right = Collections.singleton(right);
      }
      right = ((Iterable) right).iterator();

      var rightIterator = (Iterator) right;
      while (rightIterator.hasNext()) {
        var leftItem = rightIterator.next();
        var found = false;
        while (leftIterator.hasNext()) {
          var rightItem = leftIterator.next();
          if ((leftItem != null && leftItem.equals(rightItem))
              || (leftItem == null && rightItem == null)) {
            found = true;
            break;
          }
        }

        if (!found) {
          return false;
        }

        // here left iterator should go from beginning, that can be done only for iterable
        // if left at input is iterator result can be invalid
        // TODO what if left is Iterator!!!???, should we make temporary Collection , to be able to
        // iterate from beginning
        if (left instanceof Iterable) {
          leftIterator = ((Iterable) left).iterator();
        }
      }
      return true;
    }
    return false;
  }

  private boolean equalsInContainsSpace(DatabaseSessionInternal session, Object left,
      Object right) {
    if (left == null && right == null) {
      return true;
    } else {
      return QueryOperatorEquals.equals(session, left, right);
    }
  }

  @Override
  public boolean evaluate(Identifiable currentRecord, CommandContext ctx) {
    var leftValue = left.execute(currentRecord, ctx);
    if (right != null) {
      var rightValue = right.execute(currentRecord, ctx);
      return execute(ctx.getDatabaseSession(), leftValue, rightValue);
    } else {
      if (!MultiValue.isMultiValue(leftValue)) {
        return false;
      }
      var iter = MultiValue.getMultiValueIterator(leftValue);
      while (iter.hasNext()) {
        var item = iter.next();
        if (item instanceof Identifiable && condition.evaluate((Identifiable) item, ctx)) {
          return true;
        } else if (item instanceof Result && condition.evaluate((Result) item, ctx)) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean evaluate(Result currentRecord, CommandContext ctx) {
    if (left.isFunctionAny()) {
      return evaluateAny(currentRecord, ctx);
    }

    if (left.isFunctionAll()) {
      return evaluateAllFunction(currentRecord, ctx);
    }

    var leftValue = left.execute(currentRecord, ctx);
    if (right != null) {
      var rightValue = right.execute(currentRecord, ctx);
      return execute(ctx.getDatabaseSession(), leftValue, rightValue);
    } else {
      if (!MultiValue.isMultiValue(leftValue)) {
        return false;
      }
      var iter = MultiValue.getMultiValueIterator(leftValue);
      while (iter.hasNext()) {
        var item = iter.next();
        if (item instanceof Identifiable && condition.evaluate((Identifiable) item, ctx)) {
          return true;
        } else if (item instanceof Result && condition.evaluate((Result) item, ctx)) {
          return true;
        } else if (item instanceof Map) {
          var res = new ResultInternal(ctx.getDatabaseSession());
          ((Map<String, Object>) item)
              .entrySet()
              .forEach(x -> res.setProperty(x.getKey(), x.getValue()));
          if (condition.evaluate(res, ctx)) {
            return true;
          }
        }
      }
      return false;
    }
  }

  private boolean evaluateAny(Result currentRecord, CommandContext ctx) {
    if (right != null) {
      for (var s : currentRecord.getPropertyNames()) {
        var leftVal = currentRecord.getProperty(s);
        var rightValue = right.execute(currentRecord, ctx);
        if (execute(ctx.getDatabaseSession(), leftVal, rightValue)) {
          return true;
        }
      }
      return false;
    } else {
      for (var s : currentRecord.getPropertyNames()) {
        var leftValue = currentRecord.getProperty(s);

        if (!MultiValue.isMultiValue(leftValue)) {
          continue;
        }
        var iter = MultiValue.getMultiValueIterator(leftValue);
        while (iter.hasNext()) {
          var item = iter.next();
          if (item instanceof Identifiable && condition.evaluate((Identifiable) item, ctx)) {
            return true;
          } else if (item instanceof Result && condition.evaluate((Result) item, ctx)) {
            return true;
          } else if (item instanceof Map) {
            var res = new ResultInternal(ctx.getDatabaseSession());
            ((Map<String, Object>) item)
                .entrySet()
                .forEach(x -> res.setProperty(x.getKey(), x.getValue()));
            if (condition.evaluate(res, ctx)) {
              return true;
            }
          }
        }
      }
      return false;
    }
  }

  private boolean evaluateAllFunction(Result currentRecord, CommandContext ctx) {
    if (right != null) {
      for (var s : currentRecord.getPropertyNames()) {
        var leftVal = currentRecord.getProperty(s);
        var rightValue = right.execute(currentRecord, ctx);
        if (!execute(ctx.getDatabaseSession(), leftVal, rightValue)) {
          return false;
        }
      }
      return true;
    } else {
      for (var s : currentRecord.getPropertyNames()) {
        var leftValue = currentRecord.getProperty(s);

        if (!MultiValue.isMultiValue(leftValue)) {
          return false;
        }
        var iter = MultiValue.getMultiValueIterator(leftValue);
        var found = false;
        while (iter.hasNext()) {
          var item = iter.next();
          if (item instanceof Identifiable && condition.evaluate((Identifiable) item, ctx)) {
            found = true;
            break;
          } else if (item instanceof Result && condition.evaluate((Result) item, ctx)) {
            found = true;
            break;
          } else if (item instanceof Map) {
            var res = new ResultInternal(ctx.getDatabaseSession());
            ((Map<String, Object>) item)
                .entrySet()
                .forEach(x -> res.setProperty(x.getKey(), x.getValue()));
            if (condition.evaluate(res, ctx)) {
              found = true;
              break;
            }
          }
        }
        if (!found) {
          return false;
        }
      }
      return true;
    }
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    left.toString(params, builder);
    builder.append(" CONTAINS ");
    if (right != null) {
      right.toString(params, builder);
    } else if (condition != null) {
      builder.append("(");
      condition.toString(params, builder);
      builder.append(")");
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    left.toGenericStatement(builder);
    builder.append(" CONTAINS ");
    if (right != null) {
      right.toGenericStatement(builder);
    } else if (condition != null) {
      builder.append("(");
      condition.toGenericStatement(builder);
      builder.append(")");
    }
  }

  @Override
  public boolean supportsBasicCalculation() {
    if (!left.supportsBasicCalculation()) {
      return false;
    }
    if (!right.supportsBasicCalculation()) {
      return false;
    }
    return condition.supportsBasicCalculation();
  }

  @Override
  protected int getNumberOfExternalCalculations() {
    var total = 0;
    if (condition != null) {
      total += condition.getNumberOfExternalCalculations();
    }
    if (!left.supportsBasicCalculation()) {
      total++;
    }
    if (right != null && !right.supportsBasicCalculation()) {
      total++;
    }
    return total;
  }

  @Override
  protected List<Object> getExternalCalculationConditions() {
    List<Object> result = new ArrayList<Object>();

    if (condition != null) {
      result.addAll(condition.getExternalCalculationConditions());
    }
    if (!left.supportsBasicCalculation()) {
      result.add(left);
    }
    if (right != null && !right.supportsBasicCalculation()) {
      result.add(right);
    }
    return result;
  }

  @Override
  public boolean needsAliases(Set<String> aliases) {
    if (left != null && left.needsAliases(aliases)) {
      return true;
    }
    if (right != null && right.needsAliases(aliases)) {
      return true;
    }
    return condition != null && condition.needsAliases(aliases);
  }

  @Override
  public SQLContainsCondition copy() {
    var result = new SQLContainsCondition(-1);
    result.left = left == null ? null : left.copy();
    result.right = right == null ? null : right.copy();
    result.condition = condition == null ? null : condition.copy();
    return result;
  }

  @Override
  public void extractSubQueries(SubQueryCollector collector) {
    if (left != null) {
      left.extractSubQueries(collector);
    }
    if (right != null) {
      right.extractSubQueries(collector);
    }
    if (condition != null) {
      condition.extractSubQueries(collector);
    }
  }

  @Override
  public boolean refersToParent() {
    if (left != null && left.refersToParent()) {
      return true;
    }
    if (right != null && right.refersToParent()) {
      return true;
    }
    return condition != null && condition.refersToParent();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    var that = (SQLContainsCondition) o;

    if (!Objects.equals(left, that.left)) {
      return false;
    }
    if (!Objects.equals(right, that.right)) {
      return false;
    }
    return Objects.equals(condition, that.condition);
  }

  @Override
  public int hashCode() {
    var result = left != null ? left.hashCode() : 0;
    result = 31 * result + (right != null ? right.hashCode() : 0);
    result = 31 * result + (condition != null ? condition.hashCode() : 0);
    return result;
  }

  @Override
  public List<String> getMatchPatternInvolvedAliases() {
    var leftX = left == null ? null : left.getMatchPatternInvolvedAliases();
    var rightX = right == null ? null : right.getMatchPatternInvolvedAliases();
    var conditionX = condition == null ? null : condition.getMatchPatternInvolvedAliases();

    List<String> result = new ArrayList<String>();
    if (leftX != null) {
      result.addAll(leftX);
    }
    if (rightX != null) {
      result.addAll(rightX);
    }
    if (conditionX != null) {
      result.addAll(conditionX);
    }

    return result.size() == 0 ? null : result;
  }

  @Override
  public boolean isCacheable(DatabaseSessionInternal session) {
    if (left != null && !left.isCacheable(session)) {
      return false;
    }
    if (right != null && !right.isCacheable(session)) {
      return false;
    }
    return condition == null || condition.isCacheable(session);
  }
}
/* JavaCC - OriginalChecksum=bad1118296ea74860e88d66bfe9fa222 (do not edit this line) */
