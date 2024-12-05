package com.orientechnologies.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.core.sql.executor.resultset.OProduceExecutionStream;
import com.orientechnologies.core.sql.parser.OExpression;
import com.orientechnologies.core.sql.parser.OIdentifier;
import com.orientechnologies.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.core.sql.parser.OInsertBody;
import com.orientechnologies.core.sql.parser.OInsertSetExpression;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class InsertIntoIndexStep extends AbstractExecutionStep {

  private final OIndexIdentifier targetIndex;
  private final OInsertBody body;

  public InsertIntoIndexStep(
      OIndexIdentifier targetIndex,
      OInsertBody insertBody,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetIndex = targetIndex;
    this.body = insertBody;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new OProduceExecutionStream(this::produce).limit(1);
  }

  private YTResultInternal produce(OCommandContext ctx) {
    final YTDatabaseSessionInternal database = ctx.getDatabase();
    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, targetIndex.getIndexName());
    if (index == null) {
      throw new YTCommandExecutionException("Index not found: " + targetIndex);
    }
    List<OInsertSetExpression> setExps = body.getSetExpressions();
    if (body.getContent() != null) {
      throw new YTCommandExecutionException(
          "Invalid expression: INSERT INTO INDEX:... CONTENT ...");
    }
    long count;
    if (setExps != null) {
      count = handleSet(setExps, index, ctx);
    } else {
      count = handleKeyValues(body.getIdentifierList(), body.getValueExpressions(), index, ctx);
    }

    YTResultInternal result = new YTResultInternal(database);
    result.setProperty("count", count);
    return result;
  }

  private long handleKeyValues(
      List<OIdentifier> identifierList,
      List<List<OExpression>> setExpressions,
      OIndex index,
      OCommandContext ctx) {
    OExpression keyExp = null;
    OExpression valueExp = null;
    if (identifierList == null || setExpressions == null) {
      throw new YTCommandExecutionException("Invalid insert expression");
    }
    long count = 0;
    for (List<OExpression> valList : setExpressions) {
      if (identifierList.size() != valList.size()) {
        throw new YTCommandExecutionException("Invalid insert expression");
      }
      for (int i = 0; i < identifierList.size(); i++) {
        OIdentifier key = identifierList.get(i);
        if (key.getStringValue().equalsIgnoreCase("key")) {
          keyExp = valList.get(i);
        }
        if (key.getStringValue().equalsIgnoreCase("rid")) {
          valueExp = valList.get(i);
        }
      }
      assert valueExp != null;
      assert keyExp != null;
      count += doExecute(index, ctx, keyExp, valueExp);
    }
    if (keyExp == null) {
      throw new YTCommandExecutionException("Invalid insert expression");
    }
    return count;
  }

  private long handleSet(List<OInsertSetExpression> setExps, OIndex index, OCommandContext ctx) {
    OExpression keyExp = null;
    OExpression valueExp = null;
    for (OInsertSetExpression exp : setExps) {
      if (exp.getLeft().getStringValue().equalsIgnoreCase("key")) {
        keyExp = exp.getRight();
      } else if (exp.getLeft().getStringValue().equalsIgnoreCase("rid")) {
        valueExp = exp.getRight();
      } else {
        throw new YTCommandExecutionException("Cannot set " + exp + " on index");
      }
    }
    if (keyExp == null || valueExp == null) {
      throw new YTCommandExecutionException("Invalid insert expression");
    }
    return doExecute(index, ctx, keyExp, valueExp);
  }

  private long doExecute(
      OIndex index, OCommandContext ctx, OExpression keyExp, OExpression valueExp) {
    long count = 0;
    Object key = keyExp.execute((YTResult) null, ctx);
    Object value = valueExp.execute((YTResult) null, ctx);
    if (value instanceof YTIdentifiable) {
      insertIntoIndex(ctx.getDatabase(), index, key, (YTIdentifiable) value);
      count++;
    } else if (value instanceof YTResult && ((YTResult) value).isEntity()) {
      insertIntoIndex(ctx.getDatabase(), index, key, ((YTResult) value).toEntity());
      count++;
    } else if (value instanceof YTResultSet) {
      ((YTResultSet) value).entityStream().forEach(x -> index.put(ctx.getDatabase(), key, x));
    } else if (OMultiValue.isMultiValue(value)) {
      Iterator<?> iterator = OMultiValue.getMultiValueIterator(value);
      while (iterator.hasNext()) {
        Object item = iterator.next();
        if (item instanceof YTIdentifiable) {
          insertIntoIndex(ctx.getDatabase(), index, key, (YTIdentifiable) item);
          count++;
        } else if (item instanceof YTResult && ((YTResult) item).isEntity()) {
          insertIntoIndex(ctx.getDatabase(), index, key, ((YTResult) item).toEntity());
          count++;
        } else {
          throw new YTCommandExecutionException("Cannot insert into index " + item);
        }
      }
    }
    return count;
  }

  private void insertIntoIndex(YTDatabaseSessionInternal session, final OIndex index,
      final Object key, final YTIdentifiable value) {
    index.put(session, key, value);
  }
}
