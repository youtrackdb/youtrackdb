package com.jetbrains.youtrack.db.internal.lucene.functions;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.functions.IndexableSQLFunction;
import com.jetbrains.youtrack.db.internal.core.sql.functions.SQLFunctionAbstract;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBinaryCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromClause;
import com.jetbrains.youtrack.db.internal.lucene.collections.LuceneResultSet;
import com.jetbrains.youtrack.db.internal.lucene.index.LuceneFullTextIndex;
import java.util.Map;

/**
 *
 */
public abstract class LuceneSearchFunctionTemplate extends SQLFunctionAbstract
    implements IndexableSQLFunction {

  public LuceneSearchFunctionTemplate(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public boolean canExecuteInline(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {
    return allowsIndexedExecution(target, operator, rightValue, ctx, args);
  }

  @Override
  public boolean allowsIndexedExecution(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {
    LuceneFullTextIndex index = searchForIndex(target, ctx, args);
    return index != null;
  }

  @Override
  public boolean shouldExecuteAfterSearch(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {
    return false;
  }

  @Override
  public long estimate(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {

    Iterable<Identifiable> a = searchFromTarget(target, operator, rightValue, ctx, args);
    if (a instanceof LuceneResultSet) {
      return ((LuceneResultSet) a).size();
    }
    long count = 0;
    for (Object o : a) {
      count++;
    }

    return count;
  }

  protected Map<String, ?> getMetadata(SQLExpression metadata, CommandContext ctx) {
    final Object md = metadata.execute((Identifiable) null, ctx);
    if (md instanceof EntityImpl document) {
      return document.toMap();
    } else if (md instanceof Map map) {
      return map;
    } else if (md instanceof String) {
      var doc = new EntityImpl();
      doc.fromJSON((String) md);
      return doc.toMap();
    } else {
      var doc = new EntityImpl();
      doc.fromJSON(metadata.toString());
      return doc.toMap();
    }
  }

  protected abstract LuceneFullTextIndex searchForIndex(
      SQLFromClause target, CommandContext ctx, SQLExpression... args);
}
