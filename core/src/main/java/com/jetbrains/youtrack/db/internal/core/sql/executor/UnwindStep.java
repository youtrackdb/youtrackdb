package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLUnwind;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * unwinds a result-set.
 */
public class UnwindStep extends AbstractExecutionStep {

  private final SQLUnwind unwind;
  private final List<String> unwindFields;

  public UnwindStep(SQLUnwind unwind, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.unwind = unwind;
    unwindFields =
        unwind.getItems().stream().map(SQLIdentifier::getStringValue).collect(Collectors.toList());
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev == null) {
      throw new YTCommandExecutionException("Cannot expand without a target");
    }

    ExecutionStream resultSet = prev.start(ctx);
    var db = ctx.getDatabase();
    return resultSet.flatMap((res, res2) -> fetchNextResults(db, res));
  }

  private ExecutionStream fetchNextResults(YTDatabaseSessionInternal db, YTResult res) {
    return ExecutionStream.resultIterator(unwind(db, res, unwindFields).iterator());
  }

  private static Collection<YTResult> unwind(YTDatabaseSessionInternal db, final YTResult doc,
      final List<String> unwindFields) {
    final List<YTResult> result = new ArrayList<>();

    if (unwindFields.isEmpty()) {
      result.add(doc);
    } else {
      String firstField = unwindFields.get(0);
      final List<String> nextFields = unwindFields.subList(1, unwindFields.size());

      Object fieldValue = doc.getProperty(firstField);
      if (fieldValue == null || fieldValue instanceof EntityImpl) {
        result.addAll(unwind(db, doc, nextFields));
        return result;
      }

      if (!(fieldValue instanceof Iterable) && !fieldValue.getClass().isArray()) {
        result.addAll(unwind(db, doc, nextFields));
        return result;
      }

      Iterator<?> iterator;
      if (fieldValue.getClass().isArray()) {
        iterator = OMultiValue.getMultiValueIterator(fieldValue);
      } else {
        iterator = ((Iterable<?>) fieldValue).iterator();
      }
      if (!iterator.hasNext()) {
        YTResultInternal unwindedDoc = new YTResultInternal(db);
        copy(doc, unwindedDoc);

        unwindedDoc.setProperty(firstField, null);
        result.addAll(unwind(db, unwindedDoc, nextFields));
      } else {
        do {
          Object o = iterator.next();
          YTResultInternal unwindedDoc = new YTResultInternal(db);
          copy(doc, unwindedDoc);
          unwindedDoc.setProperty(firstField, o);
          result.addAll(unwind(db, unwindedDoc, nextFields));
        } while (iterator.hasNext());
      }
    }

    return result;
  }

  private static void copy(YTResult from, YTResultInternal to) {
    for (String prop : from.getPropertyNames()) {
      to.setProperty(prop, from.getProperty(prop));
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ " + unwind;
  }
}
