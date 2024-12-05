package com.orientechnologies.lucene.functions;

import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OIndexableSQLFunction;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionAbstract;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBinaryCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OFromClause;
import java.util.Map;

/**
 *
 */
public abstract class OLuceneSearchFunctionTemplate extends OSQLFunctionAbstract
    implements OIndexableSQLFunction {

  public OLuceneSearchFunctionTemplate(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public boolean canExecuteInline(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    return allowsIndexedExecution(target, operator, rightValue, ctx, args);
  }

  @Override
  public boolean allowsIndexedExecution(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    OLuceneFullTextIndex index = searchForIndex(target, ctx, args);
    return index != null;
  }

  @Override
  public boolean shouldExecuteAfterSearch(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    return false;
  }

  @Override
  public long estimate(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {

    Iterable<YTIdentifiable> a = searchFromTarget(target, operator, rightValue, ctx, args);
    if (a instanceof OLuceneResultSet) {
      return ((OLuceneResultSet) a).size();
    }
    long count = 0;
    for (Object o : a) {
      count++;
    }

    return count;
  }

  protected Map<String, ?> getMetadata(OExpression metadata, OCommandContext ctx) {
    final Object md = metadata.execute((YTIdentifiable) null, ctx);
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

  protected abstract OLuceneFullTextIndex searchForIndex(
      OFromClause target, OCommandContext ctx, OExpression... args);
}
