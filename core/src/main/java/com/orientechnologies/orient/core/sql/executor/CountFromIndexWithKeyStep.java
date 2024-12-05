package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;

/**
 * Returns the number of records contained in an index
 */
public class CountFromIndexWithKeyStep extends AbstractExecutionStep {

  private final OIndexIdentifier target;
  private final String alias;
  private final OExpression keyValue;

  /**
   * @param targetIndex      the index name as it is parsed by the SQL parsed
   * @param alias            the name of the property returned in the result-set
   * @param ctx              the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountFromIndexWithKeyStep(
      OIndexIdentifier targetIndex,
      OExpression keyValue,
      String alias,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.target = targetIndex;
    this.alias = alias;
    this.keyValue = keyValue;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new OProduceExecutionStream(this::produce).limit(1);
  }

  private YTResult produce(OCommandContext ctx) {
    OIndex idx = ctx.getDatabase().getMetadata().getIndexManager().getIndex(target.getIndexName());
    var db = ctx.getDatabase();
    Object val =
        idx.getDefinition()
            .createValue(db, keyValue.execute(new YTResultInternal(db), ctx));
    long size = idx.getInternal().getRids(db, val).distinct().count();
    YTResultInternal result = new YTResultInternal(db);
    result.setProperty(alias, size);
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CALCULATE INDEX SIZE BY KEY: " + target;
  }
}
