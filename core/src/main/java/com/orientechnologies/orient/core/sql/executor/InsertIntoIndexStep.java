package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.orient.core.sql.parser.OInsertBody;
import com.orientechnologies.orient.core.sql.parser.OInsertSetExpression;
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
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new OProduceExecutionStream(this::produce).limit(1);
  }

  private OResultInternal produce(OCommandContext ctx) {
    final YTDatabaseSessionInternal database = ctx.getDatabase();
    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, targetIndex.getIndexName());
    if (index == null) {
      throw new OCommandExecutionException("Index not found: " + targetIndex);
    }
    List<OInsertSetExpression> setExps = body.getSetExpressions();
    if (body.getContent() != null) {
      throw new OCommandExecutionException("Invalid expression: INSERT INTO INDEX:... CONTENT ...");
    }
    long count;
    if (setExps != null) {
      count = handleSet(setExps, index, ctx);
    } else {
      count = handleKeyValues(body.getIdentifierList(), body.getValueExpressions(), index, ctx);
    }

    OResultInternal result = new OResultInternal(database);
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
      throw new OCommandExecutionException("Invalid insert expression");
    }
    long count = 0;
    for (List<OExpression> valList : setExpressions) {
      if (identifierList.size() != valList.size()) {
        throw new OCommandExecutionException("Invalid insert expression");
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
      throw new OCommandExecutionException("Invalid insert expression");
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
        throw new OCommandExecutionException("Cannot set " + exp + " on index");
      }
    }
    if (keyExp == null || valueExp == null) {
      throw new OCommandExecutionException("Invalid insert expression");
    }
    return doExecute(index, ctx, keyExp, valueExp);
  }

  private long doExecute(
      OIndex index, OCommandContext ctx, OExpression keyExp, OExpression valueExp) {
    long count = 0;
    Object key = keyExp.execute((OResult) null, ctx);
    Object value = valueExp.execute((OResult) null, ctx);
    if (value instanceof YTIdentifiable) {
      insertIntoIndex(ctx.getDatabase(), index, key, (YTIdentifiable) value);
      count++;
    } else if (value instanceof OResult && ((OResult) value).isElement()) {
      insertIntoIndex(ctx.getDatabase(), index, key, ((OResult) value).toElement());
      count++;
    } else if (value instanceof OResultSet) {
      ((OResultSet) value).elementStream().forEach(x -> index.put(ctx.getDatabase(), key, x));
    } else if (OMultiValue.isMultiValue(value)) {
      Iterator<?> iterator = OMultiValue.getMultiValueIterator(value);
      while (iterator.hasNext()) {
        Object item = iterator.next();
        if (item instanceof YTIdentifiable) {
          insertIntoIndex(ctx.getDatabase(), index, key, (YTIdentifiable) item);
          count++;
        } else if (item instanceof OResult && ((OResult) item).isElement()) {
          insertIntoIndex(ctx.getDatabase(), index, key, ((OResult) item).toElement());
          count++;
        } else {
          throw new OCommandExecutionException("Cannot insert into index " + item);
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
