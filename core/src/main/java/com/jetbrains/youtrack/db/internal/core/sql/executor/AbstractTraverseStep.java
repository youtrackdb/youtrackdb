package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLInteger;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLTraverseProjectionItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLWhereClause;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class AbstractTraverseStep extends AbstractExecutionStep {

  protected final SQLWhereClause whileClause;
  protected final List<SQLTraverseProjectionItem> projections;
  protected final SQLInteger maxDepth;

  public AbstractTraverseStep(
      List<SQLTraverseProjectionItem> projections,
      SQLWhereClause whileClause,
      SQLInteger maxDepth,
      CommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.whileClause = whileClause;
    this.maxDepth = maxDepth;

    try (final var stream = projections.stream()) {
      this.projections = stream.map(SQLTraverseProjectionItem::copy).collect(Collectors.toList());
    }
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    assert prev != null;

    var resultSet = prev.start(ctx);
    return new ExecutionStream() {
      private final List<Result> entryPoints = new ArrayList<>();
      private final List<Result> results = new ArrayList<>();
      private final Set<RID> traversed = new RidSet();

      @Override
      public boolean hasNext(CommandContext ctx) {
        if (results.isEmpty()) {
          fetchNextBlock(ctx, this.entryPoints, this.results, this.traversed, resultSet);
        }
        return !results.isEmpty();
      }

      @Override
      public Result next(CommandContext ctx) {
        if (!hasNext(ctx)) {
          throw new IllegalStateException();
        }
        var result = results.removeFirst();
        if (result.isEntity()) {
          this.traversed.add(result.asEntity().getIdentity());
        }
        return result;
      }

      @Override
      public void close(CommandContext ctx) {
      }
    };
  }

  private void fetchNextBlock(
      CommandContext ctx,
      List<Result> entryPoints,
      List<Result> results,
      Set<RID> traversed,
      ExecutionStream resultSet) {
    if (!results.isEmpty()) {
      return;
    }
    while (true) {
      if (entryPoints.isEmpty()) {
        fetchNextEntryPoints(resultSet, ctx, entryPoints, traversed);
      }
      if (entryPoints.isEmpty()) {
        return;
      }
      fetchNextResults(ctx, results, entryPoints, traversed);
      if (!results.isEmpty()) {
        return;
      }
    }
  }

  protected abstract void fetchNextEntryPoints(
      ExecutionStream toFetch,
      CommandContext ctx,
      List<Result> entryPoints,
      Set<RID> traversed);

  protected abstract void fetchNextResults(
      CommandContext ctx, List<Result> results, List<Result> entryPoints,
      Set<RID> traversed);

  @Override
  public String toString() {
    return prettyPrint(0, 2);
  }
}
