package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.index.OIndexDefinition;
import com.orientechnologies.core.index.OIndexInternal;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStreamProducer;
import com.orientechnologies.core.sql.executor.resultset.OMultipleExecutionStream;
import com.orientechnologies.core.sql.parser.OAndBlock;
import com.orientechnologies.core.sql.parser.OBetweenCondition;
import com.orientechnologies.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.core.sql.parser.OBinaryCondition;
import com.orientechnologies.core.sql.parser.OBooleanExpression;
import com.orientechnologies.core.sql.parser.OCollection;
import com.orientechnologies.core.sql.parser.OEqualsCompareOperator;
import com.orientechnologies.core.sql.parser.OExpression;
import com.orientechnologies.core.sql.parser.OGeOperator;
import com.orientechnologies.core.sql.parser.OGtOperator;
import com.orientechnologies.core.sql.parser.OLeOperator;
import com.orientechnologies.core.sql.parser.OLtOperator;
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

  protected final OIndexInternal index;
  private final OBinaryCondition additional;
  private final OBooleanExpression ridCondition;
  private final boolean orderAsc;

  private final OBooleanExpression condition;

  public DeleteFromIndexStep(
      OIndex index,
      OBooleanExpression condition,
      OBinaryCondition additionalRangeCondition,
      OBooleanExpression ridCondition,
      OCommandContext ctx,
      boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, ridCondition, true, ctx, profilingEnabled);
  }

  private DeleteFromIndexStep(
      OIndex index,
      OBooleanExpression condition,
      OBinaryCondition additionalRangeCondition,
      OBooleanExpression ridCondition,
      boolean orderAsc,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index.getInternal();
    this.condition = condition;
    this.additional = additionalRangeCondition;
    this.ridCondition = ridCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    var session = ctx.getDatabase();
    Set<Stream<ORawPair<Object, YTRID>>> streams = init(session, condition);
    OExecutionStreamProducer res =
        new OExecutionStreamProducer() {
          private final Iterator<Stream<ORawPair<Object, YTRID>>> iter = streams.iterator();

          @Override
          public OExecutionStream next(OCommandContext ctx) {
            Stream<ORawPair<Object, YTRID>> s = iter.next();
            return OExecutionStream.resultIterator(
                s.filter(
                        (entry) -> {
                          return filter(entry, ctx);
                        })
                    .map((nextEntry) -> readResult(session, nextEntry))
                    .iterator());
          }

          @Override
          public boolean hasNext(OCommandContext ctx) {
            return iter.hasNext();
          }

          @Override
          public void close(OCommandContext ctx) {
            while (iter.hasNext()) {
              iter.next().close();
            }
          }
        };
    return new OMultipleExecutionStream(res);
  }

  private YTResult readResult(YTDatabaseSessionInternal session, ORawPair<Object, YTRID> entry) {
    YTResultInternal result = new YTResultInternal(session);
    YTRID value = entry.second;
    index.remove(session, entry.first, value);
    return result;
  }

  private boolean filter(ORawPair<Object, YTRID> entry, OCommandContext ctx) {
    if (ridCondition != null) {
      YTResultInternal res = new YTResultInternal(ctx.getDatabase());
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

  private Set<Stream<ORawPair<Object, YTRID>>> init(YTDatabaseSessionInternal session,
      OBooleanExpression condition) {
    Set<Stream<ORawPair<Object, YTRID>>> acquiredStreams =
        Collections.newSetFromMap(new IdentityHashMap<>());
    if (index.getDefinition() == null) {
      return acquiredStreams;
    }
    if (condition == null) {
      processFlatIteration(session, acquiredStreams);
    } else if (condition instanceof OBinaryCondition) {
      processBinaryCondition(acquiredStreams);
    } else if (condition instanceof OBetweenCondition) {
      processBetweenCondition(session, acquiredStreams);
    } else if (condition instanceof OAndBlock) {
      processAndBlock(acquiredStreams);
    } else {
      throw new YTCommandExecutionException(
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
  private void processAndBlock(Set<Stream<ORawPair<Object, YTRID>>> acquiredStreams) {
    OCollection fromKey = indexKeyFrom((OAndBlock) condition, additional);
    OCollection toKey = indexKeyTo((OAndBlock) condition, additional);
    boolean fromKeyIncluded = indexKeyFromIncluded((OAndBlock) condition, additional);
    boolean toKeyIncluded = indexKeyToIncluded((OAndBlock) condition, additional);
    init(acquiredStreams, fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration(YTDatabaseSessionInternal session,
      Set<Stream<ORawPair<Object, YTRID>>> acquiredStreams) {
    Stream<ORawPair<Object, YTRID>> stream =
        orderAsc ? index.stream(session) : index.descStream(session);
    storeAcquiredStream(stream, acquiredStreams);
  }

  private static void storeAcquiredStream(
      Stream<ORawPair<Object, YTRID>> stream,
      Set<Stream<ORawPair<Object, YTRID>>> acquiredStreams) {
    if (stream != null) {
      acquiredStreams.add(stream);
    }
  }

  private void init(
      Set<Stream<ORawPair<Object, YTRID>>> acquiredStreams,
      OCollection fromKey,
      boolean fromKeyIncluded,
      OCollection toKey,
      boolean toKeyIncluded) {
    Object secondValue = fromKey.execute((YTResult) null, ctx);
    Object thirdValue = toKey.execute((YTResult) null, ctx);
    OIndexDefinition indexDef = index.getDefinition();
    Stream<ORawPair<Object, YTRID>> stream;
    var database = ctx.getDatabase();
    if (index.supportsOrderedIterations()) {
      stream =
          index.streamEntriesBetween(database,
              toBetweenIndexKey(database, indexDef, secondValue),
              fromKeyIncluded,
              toBetweenIndexKey(database, indexDef, thirdValue),
              toKeyIncluded, orderAsc);
      storeAcquiredStream(stream, acquiredStreams);
    } else if (additional == null && allEqualities((OAndBlock) condition)) {
      stream = index.streamEntries(database, toIndexKey(ctx.getDatabase(), indexDef, secondValue),
          orderAsc);
      storeAcquiredStream(stream, acquiredStreams);
    } else {
      throw new UnsupportedOperationException(
          "Cannot evaluate " + this.condition + " on index " + index);
    }
  }

  private static boolean allEqualities(OAndBlock condition) {
    if (condition == null) {
      return false;
    }
    for (OBooleanExpression exp : condition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        if (((OBinaryCondition) exp).getOperator() instanceof OEqualsCompareOperator) {
          return true;
        }
      } else {
        return false;
      }
    }
    return true;
  }

  private void processBetweenCondition(YTDatabaseSessionInternal session,
      Set<Stream<ORawPair<Object, YTRID>>> acquiredStreams) {
    OIndexDefinition definition = index.getDefinition();
    OExpression key = ((OBetweenCondition) condition).getFirst();
    if (!key.toString().equalsIgnoreCase("key")) {
      throw new YTCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
    OExpression second = ((OBetweenCondition) condition).getSecond();
    OExpression third = ((OBetweenCondition) condition).getThird();

    Object secondValue = second.execute((YTResult) null, ctx);
    Object thirdValue = third.execute((YTResult) null, ctx);
    Stream<ORawPair<Object, YTRID>> stream =
        index.streamEntriesBetween(session,
            toBetweenIndexKey(session, definition, secondValue),
            true,
            toBetweenIndexKey(session, definition, thirdValue),
            true, orderAsc);
    storeAcquiredStream(stream, acquiredStreams);
  }

  private void processBinaryCondition(Set<Stream<ORawPair<Object, YTRID>>> acquiredStreams) {
    OIndexDefinition definition = index.getDefinition();
    OBinaryCompareOperator operator = ((OBinaryCondition) condition).getOperator();
    OExpression left = ((OBinaryCondition) condition).getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new YTCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
    Object rightValue = ((OBinaryCondition) condition).getRight().execute((YTResult) null, ctx);
    Stream<ORawPair<Object, YTRID>> stream =
        createStream(ctx.getDatabase(), operator, definition, rightValue);
    storeAcquiredStream(stream, acquiredStreams);
  }

  private static Collection<?> toIndexKey(
      YTDatabaseSessionInternal session, OIndexDefinition definition, Object rightValue) {
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
      YTDatabaseSessionInternal session, OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      rightValue = ((Collection<?>) rightValue).iterator().next();
    }
    rightValue = definition.createValue(session, rightValue);

    if (definition.getFields().size() > 1 && !(rightValue instanceof Collection)) {
      rightValue = Collections.singleton(rightValue);
    }
    return rightValue;
  }

  private Stream<ORawPair<Object, YTRID>> createStream(
      YTDatabaseSessionInternal session,
      OBinaryCompareOperator operator,
      OIndexDefinition definition,
      Object value) {
    boolean orderAsc = this.orderAsc;
    if (operator instanceof OEqualsCompareOperator) {
      return index.streamEntries(session, toIndexKey(session, definition, value), orderAsc);
    } else if (operator instanceof OGeOperator) {
      return index.streamEntriesMajor(session, value, true, orderAsc);
    } else if (operator instanceof OGtOperator) {
      return index.streamEntriesMajor(session, value, false, orderAsc);
    } else if (operator instanceof OLeOperator) {
      return index.streamEntriesMinor(session, value, true, orderAsc);
    } else if (operator instanceof OLtOperator) {
      return index.streamEntriesMinor(session, value, false, orderAsc);
    } else {
      throw new YTCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
  }

  private static OCollection indexKeyFrom(OAndBlock keyCondition, OBinaryCondition additional) {
    OCollection result = new OCollection(-1);
    for (OBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition binaryCond) {
        OBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof OEqualsCompareOperator)
            || (operator instanceof OGtOperator)
            || (operator instanceof OGeOperator)) {
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

  private static OCollection indexKeyTo(OAndBlock keyCondition, OBinaryCondition additional) {
    OCollection result = new OCollection(-1);
    for (OBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition binaryCond) {
        OBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof OEqualsCompareOperator)
            || (operator instanceof OLtOperator)
            || (operator instanceof OLeOperator)) {
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

  private static boolean indexKeyFromIncluded(OAndBlock keyCondition, OBinaryCondition additional) {
    OBooleanExpression exp =
        keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
      OBinaryCompareOperator additionalOperator =
          Optional.ofNullable(additional).map(OBinaryCondition::getOperator).orElse(null);
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

  private static boolean isGreaterOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OGeOperator || operator instanceof OGtOperator;
  }

  private static boolean isLessOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OLeOperator || operator instanceof OLtOperator;
  }

  private static boolean isIncludeOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OGeOperator || operator instanceof OLeOperator;
  }

  private static boolean indexKeyToIncluded(OAndBlock keyCondition, OBinaryCondition additional) {
    OBooleanExpression exp =
        keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
      OBinaryCompareOperator additionalOperator =
          Optional.ofNullable(additional).map(OBinaryCondition::getOperator).orElse(null);
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
        OExecutionStepInternal.getIndent(depth, indent) + "+ DELETE FROM INDEX " + index.getName();
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
                        + OExecutionStepInternal.getIndent(depth, indent)
                        + "  "
                        + oBooleanExpression
                        + additional))
            .orElse(""));
    return result;
  }
}
