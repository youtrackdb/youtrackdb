package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class DistinctExecutionStep extends AbstractExecutionStep {

  private final long maxElementsAllowed;

  public DistinctExecutionStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    YTDatabaseSession db = ctx == null ? null : ctx.getDatabase();

    maxElementsAllowed =
        db == null
            ? YTGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong()
            : db.getConfiguration()
                .getValueAsLong(YTGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream resultSet = prev.start(ctx);
    Set<OResult> pastItems = new HashSet<>();
    ORidSet pastRids = new ORidSet();

    return resultSet.filter((result, context) -> filterMap(result, pastRids, pastItems));
  }

  private OResult filterMap(OResult result, Set<YTRID> pastRids, Set<OResult> pastItems) {
    if (alreadyVisited(result, pastRids, pastItems)) {
      return null;
    } else {
      markAsVisited(result, pastRids, pastItems);
      return result;
    }
  }

  private void markAsVisited(OResult nextValue, Set<YTRID> pastRids, Set<OResult> pastItems) {
    if (nextValue.isElement()) {
      YTRID identity = nextValue.toElement().getIdentity();
      int cluster = identity.getClusterId();
      long pos = identity.getClusterPosition();
      if (cluster >= 0 && pos >= 0) {
        pastRids.add(identity);
        return;
      }
    }
    pastItems.add(nextValue);
    if (maxElementsAllowed > 0 && maxElementsAllowed < pastItems.size()) {
      pastItems.clear();
      throw new YTCommandExecutionException(
          "Limit of allowed elements for in-heap DISTINCT in a single query exceeded ("
              + maxElementsAllowed
              + ") . You can set "
              + YTGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey()
              + " to increase this limit");
    }
  }

  private boolean alreadyVisited(OResult nextValue, Set<YTRID> pastRids, Set<OResult> pastItems) {
    if (nextValue.isElement()) {
      YTRID identity = nextValue.toElement().getIdentity();
      int cluster = identity.getClusterId();
      long pos = identity.getClusterPosition();
      if (cluster >= 0 && pos >= 0) {
        return pastRids.contains(identity);
      }
    }
    return pastItems.contains(nextValue);
  }

  @Override
  public void sendTimeout() {
  }

  @Override
  public void close() {
    if (prev != null) {
      prev.close();
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ DISTINCT";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
