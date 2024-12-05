package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ProduceExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIndexIdentifier;

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
      CommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.target = targetIndex;
    this.alias = alias;
    this.keyValue = keyValue;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new ProduceExecutionStream(this::produce).limit(1);
  }

  private YTResult produce(CommandContext ctx) {
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
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CALCULATE INDEX SIZE BY KEY: " + target;
  }
}
