package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.MultipleExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStreamProducer;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLAndBlock;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBetweenCondition;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBinaryCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBinaryCondition;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBooleanExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCollection;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLEqualsCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLGeOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLGtOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLLeOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLLtOperator;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 */
public class DeleteFromIndexStep extends AbstractExecutionStep {

  protected final IndexInternal index;
  private final SQLBinaryCondition additional;
  private final SQLBooleanExpression ridCondition;
  private final boolean orderAsc;

  private final SQLBooleanExpression condition;

  public DeleteFromIndexStep(
      Index index,
      SQLBooleanExpression condition,
      SQLBinaryCondition additionalRangeCondition,
      SQLBooleanExpression ridCondition,
      CommandContext ctx,
      boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, ridCondition, true, ctx, profilingEnabled);
  }

  private DeleteFromIndexStep(
      Index index,
      SQLBooleanExpression condition,
      SQLBinaryCondition additionalRangeCondition,
      SQLBooleanExpression ridCondition,
      boolean orderAsc,
      CommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index.getInternal();
    this.condition = condition;
    this.additional = additionalRangeCondition;
    this.ridCondition = ridCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    var session = ctx.getDatabase();
    Set<Stream<RawPair<Object, RID>>> streams = init(session, condition);
    ExecutionStreamProducer res =
        new ExecutionStreamProducer() {
          private final Iterator<Stream<RawPair<Object, RID>>> iter = streams.iterator();

          @Override
          public ExecutionStream next(CommandContext ctx) {
            Stream<RawPair<Object, RID>> s = iter.next();
            return ExecutionStream.resultIterator(
                s.filter(
                        (entry) -> {
                          return filter(entry, ctx);
                        })
                    .map((nextEntry) -> readResult(session, nextEntry))
                    .iterator());
          }

          @Override
          public boolean hasNext(CommandContext ctx) {
            return iter.hasNext();
          }

          @Override
          public void close(CommandContext ctx) {
            while (iter.hasNext()) {
              iter.next().close();
            }
          }
        };
    return new MultipleExecutionStream(res);
  }

  private Result readResult(DatabaseSessionInternal session, RawPair<Object, RID> entry) {
    ResultInternal result = new ResultInternal(session);
    RID value = entry.second;
    index.remove(session, entry.first, value);
    return result;
  }

  private boolean filter(RawPair<Object, RID> entry, CommandContext ctx) {
    if (ridCondition != null) {
      ResultInternal res = new ResultInternal(ctx.getDatabase());
      res.setProperty("rid", entry.second);
      return ridCondition.evaluate(res, ctx);
    } else {
      return true;
    }
  }

  @Override
  public void close() {
    super.close();
  }

  private Set<Stream<RawPair<Object, RID>>> init(DatabaseSessionInternal session,
      SQLBooleanExpression condition) {
    Set<Stream<RawPair<Object, RID>>> acquiredStreams =
        Collections.newSetFromMap(new IdentityHashMap<>());
    if (index.getDefinition() == null) {
      return acquiredStreams;
    }
    if (condition == null) {
      processFlatIteration(session, acquiredStreams);
    } else if (condition instanceof SQLBinaryCondition) {
      processBinaryCondition(acquiredStreams);
    } else if (condition instanceof SQLBetweenCondition) {
      processBetweenCondition(session, acquiredStreams);
    } else if (condition instanceof SQLAndBlock) {
      processAndBlock(acquiredStreams);
    } else {
      throw new CommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
    return acquiredStreams;
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be
   * ignored)
   *
   * @param acquiredStreams TODO
   */
  private void processAndBlock(Set<Stream<RawPair<Object, RID>>> acquiredStreams) {
    SQLCollection fromKey = indexKeyFrom((SQLAndBlock) condition, additional);
    SQLCollection toKey = indexKeyTo((SQLAndBlock) condition, additional);
    boolean fromKeyIncluded = indexKeyFromIncluded((SQLAndBlock) condition, additional);
    boolean toKeyIncluded = indexKeyToIncluded((SQLAndBlock) condition, additional);
    init(acquiredStreams, fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration(DatabaseSessionInternal session,
      Set<Stream<RawPair<Object, RID>>> acquiredStreams) {
    Stream<RawPair<Object, RID>> stream =
        orderAsc ? index.stream(session) : index.descStream(session);
    storeAcquiredStream(stream, acquiredStreams);
  }

  private static void storeAcquiredStream(
      Stream<RawPair<Object, RID>> stream,
      Set<Stream<RawPair<Object, RID>>> acquiredStreams) {
    if (stream != null) {
      acquiredStreams.add(stream);
    }
  }

  private void init(
      Set<Stream<RawPair<Object, RID>>> acquiredStreams,
      SQLCollection fromKey,
      boolean fromKeyIncluded,
      SQLCollection toKey,
      boolean toKeyIncluded) {
    Object secondValue = fromKey.execute((Result) null, ctx);
    Object thirdValue = toKey.execute((Result) null, ctx);
    IndexDefinition indexDef = index.getDefinition();
    Stream<RawPair<Object, RID>> stream;
    var database = ctx.getDatabase();
    if (index.supportsOrderedIterations()) {
      stream =
          index.streamEntriesBetween(database,
              toBetweenIndexKey(database, indexDef, secondValue),
              fromKeyIncluded,
              toBetweenIndexKey(database, indexDef, thirdValue),
              toKeyIncluded, orderAsc);
      storeAcquiredStream(stream, acquiredStreams);
    } else if (additional == null && allEqualities((SQLAndBlock) condition)) {
      stream = index.streamEntries(database, toIndexKey(ctx.getDatabase(), indexDef, secondValue),
          orderAsc);
      storeAcquiredStream(stream, acquiredStreams);
    } else {
      throw new UnsupportedOperationException(
          "Cannot evaluate " + this.condition + " on index " + index);
    }
  }

  private static boolean allEqualities(SQLAndBlock condition) {
    if (condition == null) {
      return false;
    }
    for (SQLBooleanExpression exp : condition.getSubBlocks()) {
      if (exp instanceof SQLBinaryCondition) {
        if (((SQLBinaryCondition) exp).getOperator() instanceof SQLEqualsCompareOperator) {
          return true;
        }
      } else {
        return false;
      }
    }
    return true;
  }

  private void processBetweenCondition(DatabaseSessionInternal session,
      Set<Stream<RawPair<Object, RID>>> acquiredStreams) {
    IndexDefinition definition = index.getDefinition();
    SQLExpression key = ((SQLBetweenCondition) condition).getFirst();
    if (!key.toString().equalsIgnoreCase("key")) {
      throw new CommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
    SQLExpression second = ((SQLBetweenCondition) condition).getSecond();
    SQLExpression third = ((SQLBetweenCondition) condition).getThird();

    Object secondValue = second.execute((Result) null, ctx);
    Object thirdValue = third.execute((Result) null, ctx);
    Stream<RawPair<Object, RID>> stream =
        index.streamEntriesBetween(session,
            toBetweenIndexKey(session, definition, secondValue),
            true,
            toBetweenIndexKey(session, definition, thirdValue),
            true, orderAsc);
    storeAcquiredStream(stream, acquiredStreams);
  }

  private void processBinaryCondition(Set<Stream<RawPair<Object, RID>>> acquiredStreams) {
    IndexDefinition definition = index.getDefinition();
    SQLBinaryCompareOperator operator = ((SQLBinaryCondition) condition).getOperator();
    SQLExpression left = ((SQLBinaryCondition) condition).getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new CommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
    Object rightValue = ((SQLBinaryCondition) condition).getRight().execute((Result) null, ctx);
    Stream<RawPair<Object, RID>> stream =
        createStream(ctx.getDatabase(), operator, definition, rightValue);
    storeAcquiredStream(stream, acquiredStreams);
  }

  private static Collection<?> toIndexKey(
      DatabaseSessionInternal session, IndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      rightValue = ((Collection<?>) rightValue).iterator().next();
    }
    if (rightValue instanceof List) {
      rightValue = definition.createValue(session, (List<?>) rightValue);
    } else {
      rightValue = definition.createValue(session, rightValue);
    }
    if (!(rightValue instanceof Collection)) {
      rightValue = Collections.singleton(rightValue);
    }
    return (Collection<?>) rightValue;
  }

  private static Object toBetweenIndexKey(
      DatabaseSessionInternal session, IndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      rightValue = ((Collection<?>) rightValue).iterator().next();
    }
    rightValue = definition.createValue(session, rightValue);

    if (definition.getFields().size() > 1 && !(rightValue instanceof Collection)) {
      rightValue = Collections.singleton(rightValue);
    }
    return rightValue;
  }

  private Stream<RawPair<Object, RID>> createStream(
      DatabaseSessionInternal session,
      SQLBinaryCompareOperator operator,
      IndexDefinition definition,
      Object value) {
    boolean orderAsc = this.orderAsc;
    if (operator instanceof SQLEqualsCompareOperator) {
      return index.streamEntries(session, toIndexKey(session, definition, value), orderAsc);
    } else if (operator instanceof SQLGeOperator) {
      return index.streamEntriesMajor(session, value, true, orderAsc);
    } else if (operator instanceof SQLGtOperator) {
      return index.streamEntriesMajor(session, value, false, orderAsc);
    } else if (operator instanceof SQLLeOperator) {
      return index.streamEntriesMinor(session, value, true, orderAsc);
    } else if (operator instanceof SQLLtOperator) {
      return index.streamEntriesMinor(session, value, false, orderAsc);
    } else {
      throw new CommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
  }

  private static SQLCollection indexKeyFrom(SQLAndBlock keyCondition,
      SQLBinaryCondition additional) {
    SQLCollection result = new SQLCollection(-1);
    for (SQLBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof SQLBinaryCondition binaryCond) {
        SQLBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof SQLEqualsCompareOperator)
            || (operator instanceof SQLGtOperator)
            || (operator instanceof SQLGeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private static SQLCollection indexKeyTo(SQLAndBlock keyCondition, SQLBinaryCondition additional) {
    SQLCollection result = new SQLCollection(-1);
    for (SQLBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof SQLBinaryCondition binaryCond) {
        SQLBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof SQLEqualsCompareOperator)
            || (operator instanceof SQLLtOperator)
            || (operator instanceof SQLLeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private static boolean indexKeyFromIncluded(
      SQLAndBlock keyCondition, SQLBinaryCondition additional) {
    SQLBooleanExpression exp =
        keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof SQLBinaryCondition) {
      SQLBinaryCompareOperator operator = ((SQLBinaryCondition) exp).getOperator();
      SQLBinaryCompareOperator additionalOperator =
          Optional.ofNullable(additional).map(SQLBinaryCondition::getOperator).orElse(null);
      if (isGreaterOperator(operator)) {
        return isIncludeOperator(operator);
      } else {
        return additionalOperator == null
            || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator));
      }
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  private static boolean isGreaterOperator(SQLBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof SQLGeOperator || operator instanceof SQLGtOperator;
  }

  private static boolean isLessOperator(SQLBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof SQLLeOperator || operator instanceof SQLLtOperator;
  }

  private static boolean isIncludeOperator(SQLBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof SQLGeOperator || operator instanceof SQLLeOperator;
  }

  private static boolean indexKeyToIncluded(SQLAndBlock keyCondition,
      SQLBinaryCondition additional) {
    SQLBooleanExpression exp =
        keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof SQLBinaryCondition) {
      SQLBinaryCompareOperator operator = ((SQLBinaryCondition) exp).getOperator();
      SQLBinaryCompareOperator additionalOperator =
          Optional.ofNullable(additional).map(SQLBinaryCondition::getOperator).orElse(null);
      if (isLessOperator(operator)) {
        return isIncludeOperator(operator);
      } else {
        return additionalOperator == null
            || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator));
      }
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result =
        ExecutionStepInternal.getIndent(depth, indent) + "+ DELETE FROM INDEX " + index.getName();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    String additional =
        Optional.ofNullable(this.additional)
            .map(oBinaryCondition -> " and " + oBinaryCondition)
            .orElse("");
    result +=
        (Optional.ofNullable(condition)
            .map(
                oBooleanExpression ->
                    ("\n"
                        + ExecutionStepInternal.getIndent(depth, indent)
                        + "  "
                        + oBooleanExpression
                        + additional))
            .orElse(""));
    return result;
  }
}
